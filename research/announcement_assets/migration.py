"""Read-only inventory for legacy annual-report archives."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    build_announcement_key,
)

from .classifier import (
    AnnualReportCandidate,
    AnnualReportClassification,
    select_effective_candidate,
)
from .config import AnnouncementAssetConfig, LegacyArchiveRegistryConfig
from .models import (
    CLASSIFICATION_VOCABULARY_VERSION,
    AnnualReportVariant,
    AssetAvailability,
    DocumentFamily,
    EffectiveAnnualReport,
    EffectiveDecisionState,
    IntegrityStatus,
    OfficialAssetRecoveryManifestEntry,
    OfficialAttachmentVersion,
    OfficialDocumentBlob,
    canonical_json,
    normalize_annual_report_variant,
    normalize_document_family,
    normalize_instrument_id,
    normalize_source,
    stable_id,
    utc_now_iso,
)
from .path_segments import validate_path_segment
from .repository import AnnouncementAssetRepository
from .storage import ContentAddressedBlobStore, probe_mount_identity

_BUSINESS_FILENAME = re.compile(
    r"^(?P<symbol>\d{6})_(?P<suffix>SH|SZ|BJ)_"
    r"(?P<period>\d{4}Q[1-4])_(?P<filing>[^_]+)_"
    r"(?P<digest>[0-9a-fA-F]{64})\.pdf$"
)
_BROKER_FILENAME = re.compile(
    r"^(?P<symbol>\d{6})_(?P<period>\d{4}-\d{2}-\d{2})_"
    r"(?P<filing>[^_]+)\.pdf$"
)
_EXCHANGE_SUFFIX = {"SSE": "SH", "SZSE": "SZ", "BSE": "BJ"}


@dataclass(frozen=True)
class ArchiveInventoryItem:
    path: str
    consumer: str
    status: str
    reason: str
    instrument_id: str | None = None
    exchange: str | None = None
    report_period: str | None = None
    fiscal_year: int | None = None
    report_type: str | None = None
    source: str | None = None
    filing_id: str | None = None
    content_hash: str | None = None
    expected_hash: str | None = None
    content_length: int | None = None
    source_file_id: str | None = None
    manifest: Mapping[str, Any] = field(default_factory=dict)

    @property
    def legal_key(self) -> tuple[str, str] | None:
        if not self.source or not self.filing_id:
            return None
        return self.source, self.filing_id

    @property
    def document_family(self) -> str | None:
        """Canonical family projection retained alongside legacy report_type."""

        metadata = _manifest_classification(self.manifest)
        return normalize_document_family(
            metadata.get("document_family") or self.report_type
        )

    @property
    def variant(self) -> AnnualReportVariant | None:
        metadata = _manifest_classification(self.manifest)
        correction_evidence = bool(metadata.get("correction_evidence")) or bool(
            self.manifest.get("supersedes_source_file_id")
        )
        return normalize_annual_report_variant(
            metadata.get("variant") or self.report_type,
            correction_evidence=correction_evidence,
        )

    @property
    def is_full_report(self) -> bool:
        metadata = _manifest_classification(self.manifest)
        if "is_full_report" in metadata:
            return bool(metadata["is_full_report"])
        report_type = str(self.report_type or "").strip().lower()
        return report_type in {"annual_report", "annual_report_correction"}


@dataclass(frozen=True)
class ArchiveInventoryReport:
    items: tuple[ArchiveInventoryItem, ...]
    counts: Mapping[str, int]
    files_seen: int
    manifest_rows_seen: int
    network_requests: int = 0
    files_moved: int = 0
    files_linked: int = 0
    files_quarantined: int = 0
    files_deleted: int = 0
    root_registry_version: str | None = None
    path_template_version: str | None = None
    exclusion_policy_version: str | None = None
    inventory_fingerprint: str | None = None
    out_of_scope_directories: tuple[str, ...] = ()


@dataclass(frozen=True)
class ShadowAdoptionPeriod:
    instrument_id: str
    fiscal_year: int
    report_period: str
    status: str
    reason: str
    asset_id: str | None = None
    content_hash: str | None = None
    source_file_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class ArchiveShadowAdoptionReport:
    periods: tuple[ShadowAdoptionPeriod, ...]
    files_adopted: int
    legal_attachments_registered: int
    blobs_registered: int
    skipped_counts: Mapping[str, int]
    network_requests: int = 0
    coverage_updates: int = 0
    files_moved: int = 0
    files_linked: int = 0
    files_quarantined: int = 0
    files_deleted: int = 0


@dataclass(frozen=True)
class OrphanReconciliationReport:
    """Zero-download outcome for manifest-less legacy files."""

    resolved_paths: tuple[str, ...]
    skipped: Mapping[str, int]
    shadow_adoption: ArchiveShadowAdoptionReport | None
    network_requests: int = 0


@dataclass(frozen=True)
class ArchivePeriodReconciliation:
    instrument_id: str
    fiscal_year: int
    status: str
    reason: str
    canonical_asset_id: str | None = None
    canonical_content_hash: str | None = None
    legacy_source_file_ids: tuple[str, ...] = ()
    promotion_gate_id: str | None = None


@dataclass(frozen=True)
class ArchiveReconciliationReport:
    periods: tuple[ArchivePeriodReconciliation, ...]
    ready_for_cutover: bool
    conflict_count: int
    inventory_fingerprint: str
    config_fingerprint: str
    network_requests: int = 0
    files_moved: int = 0
    files_linked: int = 0
    files_quarantined: int = 0
    files_deleted: int = 0


@dataclass(frozen=True)
class NfsCapabilityProbe:
    source_path: str
    target_root: str
    source_filesystem_key: str
    target_filesystem_key: str
    same_filesystem: bool
    can_hardlink: bool
    can_atomic_rename: bool
    probed: bool
    reason: str


@dataclass(frozen=True)
class ArchiveConvergenceReport:
    dry_run: bool
    manifest_version: str
    entries: tuple[Mapping[str, Any], ...]
    rollback_manifest: tuple[Mapping[str, Any], ...]
    excluded_count: int
    plan_fingerprint: str
    files_moved: int = 0
    files_copied: int = 0
    files_linked: int = 0
    files_deleted: int = 0
    network_requests: int = 0


class ConvergenceMountRaceError(RuntimeError):
    """A source, target, or backup mount changed during convergence."""


def probe_nfs_capabilities(
    source_path: str | Path,
    target_root: str | Path,
    *,
    perform_probe: bool = False,
) -> NfsCapabilityProbe:
    """Probe link/rename behavior without mutating legacy archive files."""
    source = Path(source_path).resolve(strict=False)
    target = Path(target_root).resolve(strict=False)
    source_identity = probe_mount_identity(source)
    target_identity = probe_mount_identity(target)
    same_filesystem = source_identity.device_id == target_identity.device_id
    if not perform_probe:
        return NfsCapabilityProbe(
            source_path=str(source),
            target_root=str(target),
            source_filesystem_key=source_identity.filesystem_key,
            target_filesystem_key=target_identity.filesystem_key,
            same_filesystem=same_filesystem,
            can_hardlink=False,
            can_atomic_rename=same_filesystem,
            probed=False,
            reason="same_filesystem_not_probed",
        )
    if not same_filesystem:
        return NfsCapabilityProbe(
            source_path=str(source),
            target_root=str(target),
            source_filesystem_key=source_identity.filesystem_key,
            target_filesystem_key=target_identity.filesystem_key,
            same_filesystem=False,
            can_hardlink=False,
            can_atomic_rename=False,
            probed=True,
            reason="source_and_target_filesystems_differ",
        )
    target.mkdir(parents=True, exist_ok=True)
    probe_dir = Path(
        tempfile.mkdtemp(prefix="announcement-migration-probe-", dir=target)
    )
    source_probe = probe_dir / "source.part"
    link_probe = probe_dir / "link.part"
    rename_probe = probe_dir / "renamed.part"
    can_hardlink = False
    can_atomic_rename = False
    try:
        source_probe.write_bytes(b"announcement-migration-probe")
        try:
            os.link(source_probe, link_probe)
            can_hardlink = link_probe.read_bytes() == source_probe.read_bytes()
        except OSError:
            pass
        try:
            os.replace(source_probe, rename_probe)
            can_atomic_rename = (
                rename_probe.read_bytes() == b"announcement-migration-probe"
            )
        except OSError:
            pass
    finally:
        shutil.rmtree(probe_dir, ignore_errors=True)
    return NfsCapabilityProbe(
        source_path=str(source),
        target_root=str(target),
        source_filesystem_key=source_identity.filesystem_key,
        target_filesystem_key=target_identity.filesystem_key,
        same_filesystem=same_filesystem,
        can_hardlink=can_hardlink,
        can_atomic_rename=can_atomic_rename,
        probed=True,
        reason="probed",
    )


class AnnouncementArchiveInventory:
    """Classify legacy source files without mutating disk or repository state."""

    def inventory_registered(
        self,
        *,
        config: AnnouncementAssetConfig,
        manifest_rows: Iterable[Mapping[str, Any]] = (),
        fiscal_year_allowlist: Iterable[int] | None = None,
        max_files: int | None = None,
    ) -> ArchiveInventoryReport:
        """Inventory only versioned registered roots from production config."""

        return self.inventory(
            manifest_rows=manifest_rows,
            max_files=max_files,
            registry=config.legacy_inventory,
            filings_root=config.filings_root,
            known_non_legacy_roots=(config.archive_root,),
            fiscal_year_allowlist=fiscal_year_allowlist,
            enforce_registered_layout=True,
            project_root=config.project_root,
        )

    def inventory(
        self,
        *,
        business_profile_root: str | Path | None = None,
        broker_root: str | Path | None = None,
        manifest_rows: Iterable[Mapping[str, Any]] = (),
        max_files: int | None = None,
        registry: LegacyArchiveRegistryConfig | None = None,
        filings_root: str | Path | None = None,
        known_non_legacy_roots: Iterable[str | Path] = (),
        fiscal_year_allowlist: Iterable[int] | None = None,
        enforce_registered_layout: bool = False,
        project_root: str | Path | None = None,
    ) -> ArchiveInventoryReport:
        if max_files is not None and int(max_files) < 1:
            raise ValueError("max_files must be positive")
        if registry is None:
            if business_profile_root is None or broker_root is None:
                raise ValueError(
                    "inventory requires a versioned registry or both legacy roots"
                )
            registry = LegacyArchiveRegistryConfig(
                business_profile_root=Path(business_profile_root),
                broker_risk_control_root=Path(broker_root),
            )
        elif business_profile_root is not None or broker_root is not None:
            raise ValueError(
                "explicit legacy roots cannot be combined with a registry"
            )
        roots = tuple(
            (consumer, Path(root).resolve(strict=False))
            for consumer, root in registry.roots
        )
        allowlist = (
            None
            if fiscal_year_allowlist is None
            else frozenset(int(item) for item in fiscal_year_allowlist)
        )
        if allowlist is not None and not allowlist:
            raise ValueError("fiscal_year_allowlist must not be empty")
        manifests = tuple(dict(row) for row in manifest_rows)
        manifests_by_path: dict[Path, list[dict[str, Any]]] = defaultdict(list)
        outside_manifest_rows: list[tuple[Path, dict[str, Any]]] = []
        for row in manifests:
            archive_path = str(row.get("archive_path") or "").strip()
            if archive_path:
                resolved_manifest_path = Path(archive_path).resolve(strict=False)
                if enforce_registered_layout and not _path_is_beneath_any(
                    resolved_manifest_path, tuple(root for _, root in roots)
                ):
                    outside_manifest_rows.append((resolved_manifest_path, row))
                else:
                    manifests_by_path[resolved_manifest_path].append(row)

        items: list[ArchiveInventoryItem] = []
        seen_paths: set[Path] = set()
        for consumer, root in roots:
            if not root.exists():
                continue
            for path in sorted(root.rglob("*")):
                if not path.is_file():
                    continue
                if max_files is not None and len(seen_paths) >= int(max_files):
                    break
                resolved = path.resolve(strict=False)
                try:
                    resolved.relative_to(root)
                except ValueError:
                    items.append(
                        ArchiveInventoryItem(
                            path=str(path),
                            consumer=consumer,
                            status="conflicting",
                            reason="path_escapes_inventory_root",
                        )
                    )
                    continue
                seen_paths.add(resolved)
                manifest_matches = manifests_by_path.get(resolved, [])
                derived = "derived" in {part.lower() for part in path.parts}
                template_values = (
                    None
                    if not enforce_registered_layout or derived
                    else _registered_template_values(
                        path,
                        consumer=consumer,
                        registry=registry,
                    )
                )
                if enforce_registered_layout and not derived and template_values is None:
                    item = ArchiveInventoryItem(
                        path=str(path),
                        consumer=consumer,
                        status="out_of_scope",
                        reason="path_outside_registered_template",
                    )
                else:
                    parsed = self._parse_path(path, consumer=consumer)
                    template_error = (
                        None
                        if template_values is None
                        else _registered_template_identity_error(
                            template_values,
                            parsed=parsed,
                            manifests=manifest_matches,
                            consumer=consumer,
                        )
                    )
                    if template_error:
                        manifest = (
                            manifest_matches[0]
                            if len(manifest_matches) == 1
                            else {}
                        )
                        item = ArchiveInventoryItem(
                            path=str(path),
                            consumer=consumer,
                            status="out_of_scope",
                            reason=template_error,
                            source=_text(manifest.get("source")),
                            source_file_id=_text(manifest.get("source_file_id")),
                            manifest=manifest,
                            **(parsed or {}),
                        )
                    else:
                        item = self._inspect_file(
                            path,
                            consumer=consumer,
                            manifests=manifest_matches,
                        )
                if (
                    allowlist is not None
                    and item.fiscal_year is not None
                    and item.fiscal_year not in allowlist
                ):
                    item = replace(
                        item,
                        status="out_of_scope",
                        reason="fiscal_year_not_allowlisted",
                    )
                items.append(item)

        for path, row in outside_manifest_rows:
            items.append(
                ArchiveInventoryItem(
                    path=str(path),
                    consumer=str(
                        (row.get("metadata") or {}).get("consumer")
                        or "unregistered"
                    ),
                    status="out_of_scope",
                    reason="manifest_path_outside_registered_roots",
                    manifest=row,
                )
            )

        for path, rows in manifests_by_path.items():
            if path in seen_paths:
                continue
            for row in rows:
                items.append(
                    self._missing_manifest_item(path, row)
                )

        superseded_ids = {
            str(row.get("supersedes_source_file_id") or "").strip()
            for row in manifests
            if row.get("supersedes_source_file_id")
        }
        items = [
            replace(
                item,
                status="superseded",
                reason="manifest_replacement_lineage",
            )
            if item.status == "adoptable" and item.source_file_id in superseded_ids
            else item
            for item in items
        ]
        items = self._exclude_explicit_summaries(items)
        items = self._apply_duplicate_and_conflict_status(items)
        counts = Counter(item.status for item in items)
        resolved_project_root = (
            Path(project_root).resolve(strict=False)
            if project_root is not None
            else Path(
                os.path.commonpath([str(root) for _, root in roots])
            ).resolve(strict=False)
        )
        unknown_directories = (
            ()
            if filings_root is None
            else _unknown_legacy_directories(
                Path(filings_root),
                registered_roots=tuple(root for _, root in roots),
                known_roots=tuple(Path(item) for item in known_non_legacy_roots),
            )
        )
        policy_payload = {
            "registry": registry.normalized_mapping(
                project_root=resolved_project_root
            ),
            "fiscal_year_allowlist": (
                None if allowlist is None else sorted(allowlist)
            ),
            "items": [
                {
                    "path": item.path,
                    "consumer": item.consumer,
                    "status": item.status,
                    "reason": item.reason,
                    "instrument_id": item.instrument_id,
                    "exchange": item.exchange,
                    "report_period": item.report_period,
                    "fiscal_year": item.fiscal_year,
                    "report_type": item.report_type,
                    "source": item.source,
                    "filing_id": item.filing_id,
                    "content_hash": item.content_hash,
                    "expected_hash": item.expected_hash,
                    "content_length": item.content_length,
                    "source_file_id": item.source_file_id,
                    "supersedes_source_file_id": (item.manifest or {}).get(
                        "supersedes_source_file_id"
                    ),
                }
                for item in sorted(items, key=lambda value: value.path)
            ],
            "out_of_scope_directories": list(unknown_directories),
        }
        fingerprint = hashlib.sha256(
            json.dumps(
                policy_payload,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return ArchiveInventoryReport(
            items=tuple(items),
            counts=dict(sorted(counts.items())),
            files_seen=len(seen_paths),
            manifest_rows_seen=len(manifests),
            root_registry_version=registry.registry_version,
            path_template_version=registry.path_template_version,
            exclusion_policy_version=registry.exclusion_policy_version,
            inventory_fingerprint=fingerprint,
            out_of_scope_directories=unknown_directories,
        )

    def shadow_adopt(
        self,
        inventory: ArchiveInventoryReport,
        *,
        repository: AnnouncementAssetRepository,
        observed_at: str | None = None,
    ) -> ArchiveShadowAdoptionReport:
        """Register verified legacy files without network or filesystem mutation."""

        timestamp = observed_at or utc_now_iso()
        valid_items = tuple(
            item
            for item in inventory.items
            if item.status in {"adoptable", "duplicate", "superseded"}
            and item.content_hash
            and item.content_length is not None
            and item.legal_key
            and item.instrument_id
            and item.fiscal_year is not None
        )
        paths_by_hash: dict[str, tuple[str, ...]] = {}
        for content_hash in sorted({item.content_hash for item in valid_items}):
            paths_by_hash[str(content_hash)] = tuple(
                sorted(
                    {
                        item.path
                        for item in valid_items
                        if item.content_hash == content_hash
                    }
                )
            )

        legal_groups: dict[tuple[str, str], list[ArchiveInventoryItem]] = defaultdict(
            list
        )
        for item in valid_items:
            legal_groups[item.legal_key].append(item)  # type: ignore[index]

        candidates_by_period: dict[
            tuple[str, int], list[tuple[AnnualReportCandidate, ArchiveInventoryItem, str]]
        ] = defaultdict(list)
        registered_hashes: set[str] = set()
        for legal_key in sorted(legal_groups):
            group = sorted(legal_groups[legal_key], key=lambda item: item.path)
            representative = max(group, key=_legacy_item_precedence_key)
            content_hash = str(representative.content_hash)
            existing_blob = repository.get_blob(content_hash)
            canonical_path = (
                existing_blob.canonical_path
                if existing_blob is not None
                else paths_by_hash[content_hash][0]
            )
            record, attachment, classification = _legacy_record_and_attachment(
                representative,
                group=group,
                timestamp=timestamp,
            )
            announcement = repository.upsert_announcement(
                record,
                instrument_id=representative.instrument_id,
                observed_at=timestamp,
            )
            canonical_attachment = repository.upsert_attachment(
                announcement.announcement_id,
                attachment,
                observed_at=timestamp,
            )
            repository.register_blob(
                OfficialDocumentBlob(
                    content_hash=content_hash,
                    content_length=int(representative.content_length),
                    canonical_path=canonical_path,
                    signature_status="valid_pdf",
                    integrity_status=IntegrityStatus.VALID,
                    first_available_at=timestamp,
                    last_verified_at=timestamp,
                )
            )
            registered_hashes.add(content_hash)
            for legacy_item in group:
                repository.add_retention_pin(
                    blob_hash=content_hash,
                    pin_type="legacy_alias",
                    pin_key=legacy_item.path,
                    owner=legacy_item.consumer,
                    metadata={
                        "origin": "legacy_shadow_adoption",
                        "source_file_id": legacy_item.source_file_id,
                    },
                )
            observation_key = stable_id(
                "legacy_obs", canonical_attachment.attachment_id, content_hash
            )
            version_metadata = {
                "origin": "legacy_shadow_adoption",
                "legacy_paths": list(paths_by_hash[content_hash]),
                "source_file_ids": sorted(
                    {
                        item.source_file_id
                        for item in group
                        if item.source_file_id
                    }
                ),
            }
            orphan_evidence = _manifest_orphan_evidence(representative.manifest)
            if orphan_evidence is not None:
                version_metadata["orphan_reconciliation"] = orphan_evidence
            version = repository.add_attachment_version(
                OfficialAttachmentVersion(
                    version_id=stable_id(
                        "ver", canonical_attachment.attachment_id, observation_key
                    ),
                    attachment_id=canonical_attachment.attachment_id,
                    observation_key=observation_key,
                    content_hash=content_hash,
                    final_url=canonical_attachment.source_url,
                    retrieval_status="adopted",
                    integrity_status=IntegrityStatus.VALID,
                    attempt=0,
                    next_retry_at=None,
                    error_code=None,
                    observed_at=timestamp,
                    visibility_state="shadow",
                    metadata=version_metadata,
                )
            )
            candidate = AnnualReportCandidate(
                candidate_id=version.version_id,
                source=str(representative.source),
                source_announcement_id=str(representative.filing_id),
                attachment_id=canonical_attachment.attachment_id,
                content_hash=content_hash,
                published_at=_published_at(representative),
                classification=classification,
                integrity_valid=True,
                legal_chain_id=_legacy_legal_chain_id(representative),
                legal_precedence=_legacy_precedence(representative, valid_items),
            )
            if not classification.is_eligible:
                # Preserve verified non-winning evidence, such as correction
                # notices, without entering annual-report winner selection.
                continue
            scope = (str(representative.instrument_id), int(representative.fiscal_year))
            candidates_by_period[scope].append(
                (candidate, representative, announcement.announcement_id)
            )

        invalid_corrections_by_period = _invalid_correction_candidates(inventory)
        conflict_scopes = {
            (str(item.instrument_id), int(item.fiscal_year))
            for item in inventory.items
            if item.status == "conflicting"
            and item.instrument_id
            and item.fiscal_year is not None
        }
        scopes = sorted(set(candidates_by_period) | conflict_scopes)
        periods: list[ShadowAdoptionPeriod] = []
        for scope in scopes:
            instrument_id, fiscal_year = scope
            source_ids = tuple(
                sorted(
                    {
                        item.source_file_id
                        for _, item, _ in candidates_by_period.get(scope, ())
                        if item.source_file_id
                    }
                )
            )
            if scope in conflict_scopes:
                self._mark_existing_effective_unavailable(
                    repository,
                    instrument_id=instrument_id,
                    fiscal_year=fiscal_year,
                    decision_state=EffectiveDecisionState.AMBIGUOUS,
                    reason="legacy_inventory_conflict",
                    timestamp=timestamp,
                )
                periods.append(
                    ShadowAdoptionPeriod(
                        instrument_id=instrument_id,
                        fiscal_year=fiscal_year,
                        report_period=f"{fiscal_year}-12-31",
                        status="conflicting",
                        reason="legacy_inventory_conflict",
                        source_file_ids=source_ids,
                    )
                )
                continue
            rows = candidates_by_period.get(scope, [])
            valid_candidates = [candidate for candidate, _, _ in rows]
            selection = select_effective_candidate(
                [*valid_candidates, *invalid_corrections_by_period.get(scope, ())]
            )
            if selection.winner is None:
                self._mark_existing_effective_unavailable(
                    repository,
                    instrument_id=instrument_id,
                    fiscal_year=fiscal_year,
                    decision_state=selection.state,
                    reason=selection.reasons[0],
                    timestamp=timestamp,
                )
                periods.append(
                    ShadowAdoptionPeriod(
                        instrument_id=instrument_id,
                        fiscal_year=fiscal_year,
                        report_period=f"{fiscal_year}-12-31",
                        status=selection.state.value,
                        reason=selection.reasons[0],
                        source_file_ids=source_ids,
                    )
                )
                continue
            winner_row = next(
                row for row in rows if row[0].candidate_id == selection.winner.candidate_id
            )
            winner, winner_item, _ = winner_row

            # Replay only the winner's explicit legal supersedes chain.  Mere
            # publication order, equal bytes, or cross-source proximity cannot
            # create a replacement edge.
            chain_rows, chain_error = _explicit_legacy_supersedes_chain(
                rows,
                winner_row=winner_row,
                scope_items=tuple(
                    item
                    for item in valid_items
                    if item.instrument_id == instrument_id
                    and item.fiscal_year == fiscal_year
                ),
            )
            if chain_error is not None:
                self._mark_existing_effective_unavailable(
                    repository,
                    instrument_id=instrument_id,
                    fiscal_year=fiscal_year,
                    decision_state=EffectiveDecisionState.AMBIGUOUS,
                    reason=chain_error,
                    timestamp=timestamp,
                )
                periods.append(
                    ShadowAdoptionPeriod(
                        instrument_id=instrument_id,
                        fiscal_year=fiscal_year,
                        report_period=str(winner_item.report_period),
                        status="conflicting",
                        reason=chain_error,
                        source_file_ids=source_ids,
                    )
                )
                continue
            existing_decisions = repository.list_effective_decisions(
                instrument_id=instrument_id,
                fiscal_year=fiscal_year,
            )
            existing_asset_ids = {
                str(decision.replacement_asset_id)
                for decision in existing_decisions
                if decision.replacement_asset_id
            }
            committed_final: EffectiveAnnualReport | None = None
            for candidate, item, announcement_id in chain_rows:
                asset = EffectiveAnnualReport(
                    asset_id=stable_id(
                        "asset", instrument_id, fiscal_year, candidate.candidate_id
                    ),
                    instrument_id=instrument_id,
                    fiscal_year=fiscal_year,
                    report_period=str(item.report_period),
                    announcement_id=announcement_id,
                    attachment_id=candidate.attachment_id,
                    version_id=candidate.candidate_id,
                    content_hash=candidate.content_hash,
                    source=candidate.source,
                    source_announcement_id=candidate.source_announcement_id,
                    published_at=candidate.published_at,
                    variant=candidate.classification.variant
                    or AnnualReportVariant.ORIGINAL,
                    classifier_version=candidate.classification.policy_version,
                    decision_state=(
                        selection.state
                        if candidate.candidate_id == winner.candidate_id
                        else EffectiveDecisionState.CURRENT
                    ),
                    availability=AssetAvailability.LOCAL_VALID,
                    predecessor_asset_id=None,
                    pending_candidate_id=(
                        None
                        if candidate.candidate_id != winner.candidate_id
                        or selection.pending_candidate is None
                        else selection.pending_candidate.candidate_id
                    ),
                    activated_at=None,
                    last_checked_at=timestamp,
                    decision_reasons=(
                        selection.reasons
                        if candidate.candidate_id == winner.candidate_id
                        else ("legacy_correction_chain_replay",)
                    ),
                    decision_evidence={
                        "decision_policy_version": "legacy_shadow_adoption.v2",
                        "adoption_origin": "legacy_archive",
                        "source_file_id": item.source_file_id,
                    },
                    visibility_state="shadow",
                )
                if asset.asset_id in existing_asset_ids:
                    current = repository.get_effective_report(
                        instrument_id,
                        fiscal_year,
                        include_shadow=True,
                    )
                    if current is not None and current.asset_id == asset.asset_id:
                        committed = current
                    else:
                        committed = asset
                else:
                    committed, _, activated = repository.activate_effective_report(asset)
                    if not activated or committed is None:
                        raise RuntimeError(
                            "legacy correction-chain adoption lost its activation race"
                        )
                    existing_asset_ids.add(asset.asset_id)
                if candidate.candidate_id == winner.candidate_id:
                    committed_final = committed

            if committed_final is None:
                raise RuntimeError("legacy correction-chain adoption produced no winner")
            periods.append(
                ShadowAdoptionPeriod(
                    instrument_id=instrument_id,
                    fiscal_year=fiscal_year,
                    report_period=str(winner_item.report_period),
                    status=selection.state.value,
                    reason=selection.reasons[0],
                    asset_id=committed_final.asset_id,
                    content_hash=committed_final.content_hash,
                    source_file_ids=source_ids,
                )
            )

        skipped = Counter(
            item.status
            for item in inventory.items
            if item.status not in {"adoptable", "duplicate", "superseded"}
        )
        return ArchiveShadowAdoptionReport(
            periods=tuple(periods),
            files_adopted=len(valid_items),
            legal_attachments_registered=len(legal_groups),
            blobs_registered=len(registered_hashes),
            skipped_counts=dict(sorted(skipped.items())),
        )

    def reconcile_orphans(
        self,
        inventory: ArchiveInventoryReport,
        *,
        repository: AnnouncementAssetRepository,
        official_metadata: Iterable[Mapping[str, Any]] = (),
        audited_operator_mappings: Iterable[Mapping[str, Any]] = (),
        observed_at: str | None = None,
    ) -> OrphanReconciliationReport:
        """Resolve fully evidenced orphan PDFs without downloading bytes."""

        mapping_candidates_by_path: dict[
            str, list[tuple[dict[str, Any], str]]
        ] = defaultdict(list)
        skipped: Counter[str] = Counter()
        for records, evidence_kind in (
            (official_metadata, "official_metadata"),
            (audited_operator_mappings, "audited_operator_mapping"),
        ):
            for raw in records:
                mapping = dict(raw)
                raw_path = str(
                    mapping.get("path") or mapping.get("archive_path") or ""
                ).strip()
                if not raw_path:
                    skipped["missing_path"] += 1
                    continue
                path = str(Path(raw_path).resolve(strict=False))
                mapping_candidates_by_path[path].append((mapping, evidence_kind))

        # A path is only adoptable when exactly one independent mapping exists.
        # Never let input ordering choose between official and operator evidence,
        # or between two conflicting claims for the same orphan bytes.
        mapping_by_path: dict[str, tuple[dict[str, Any], str]] = {}
        conflicted_mapping_paths: set[str] = set()
        for path, candidates in mapping_candidates_by_path.items():
            if len(candidates) != 1:
                conflicted_mapping_paths.add(path)
                skipped["mapping_conflict"] += 1
                continue
            mapping_by_path[path] = candidates[0]

        resolved_items: list[ArchiveInventoryItem] = []
        orphan_paths = {
            str(Path(item.path).resolve(strict=False))
            for item in inventory.items
            if item.status == "orphan"
        }
        for path in sorted(orphan_paths):
            if path in conflicted_mapping_paths:
                continue
            evidence = mapping_by_path.get(path)
            if evidence is None:
                skipped["missing_evidence"] += 1
                continue
            mapping, evidence_kind = evidence
            reason = _validate_orphan_evidence(
                mapping, evidence_kind=evidence_kind
            )
            if reason:
                skipped[reason] += 1
                continue
            local = Path(path)
            try:
                stat = local.stat()
                with local.open("rb") as handle:
                    if handle.read(5) != b"%PDF-":
                        raise ValueError("invalid_pdf_signature")
                    handle.seek(0)
                    digest = hashlib.sha256()
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
                actual_hash = digest.hexdigest()
            except FileNotFoundError:
                skipped["missing_file"] += 1
                continue
            except (OSError, ValueError) as exc:
                skipped[f"local_invalid:{exc}"] += 1
                continue
            if actual_hash != str(mapping["content_hash"]).lower():
                skipped["content_hash_mismatch"] += 1
                continue
            if stat.st_size != int(mapping["content_length"]):
                skipped["content_length_mismatch"] += 1
                continue
            classification = _orphan_classification(mapping)
            if not classification.is_eligible:
                skipped["classification_not_eligible"] += 1
                continue
            source = normalize_source(str(mapping["source"]))
            filing_id = str(
                mapping.get("source_announcement_id") or mapping.get("filing_id")
            ).strip()
            source_file_id = str(
                mapping.get("source_file_id")
                or stable_id("orphan_source_file", source, filing_id, actual_hash)
            )
            evidence_record = _orphan_evidence_record(
                mapping, evidence_kind=evidence_kind
            )
            if evidence_record is None:
                # Keep this defensive check adjacent to the adoption boundary:
                # future validation changes must not turn unverifiable evidence
                # into a shadow record by accident.
                skipped["evidence_not_verifiable"] += 1
                continue
            manifest = {
                "source_file_id": source_file_id,
                "filing_id": filing_id,
                "attachment_id": str(mapping["attachment_id"]),
                "source": source,
                "instrument_id": normalize_instrument_id(str(mapping["instrument_id"])),
                "exchange": str(mapping["exchange"]).upper(),
                "report_period": str(mapping["report_period"]),
                "report_type": "annual_report",
                "content_hash": actual_hash,
                "content_length": stat.st_size,
                "published_at": mapping.get("published_at"),
                "source_url": mapping.get("source_url"),
                "title": mapping.get("title"),
                "metadata": {
                    "asset_classification": _classification_payload(classification),
                    "orphan_reconciliation": {
                        **evidence_record,
                    },
                },
            }
            resolved_items.append(
                ArchiveInventoryItem(
                    path=path,
                    consumer=str(mapping.get("consumer") or "orphan_reconciliation"),
                    status="adoptable",
                    reason="orphan_reconciled",
                    instrument_id=normalize_instrument_id(str(mapping["instrument_id"])),
                    exchange=str(mapping["exchange"]).upper(),
                    report_period=str(mapping["report_period"]),
                    fiscal_year=int(mapping["fiscal_year"]),
                    report_type="annual_report",
                    source=source,
                    filing_id=filing_id,
                    content_hash=actual_hash,
                    content_length=stat.st_size,
                    source_file_id=source_file_id,
                    manifest=manifest,
                )
            )

        adoption = None
        if resolved_items:
            shadow_inventory = ArchiveInventoryReport(
                items=tuple(resolved_items),
                counts={"adoptable": len(resolved_items)},
                files_seen=len(resolved_items),
                manifest_rows_seen=len(resolved_items),
                inventory_fingerprint=hashlib.sha256(
                    json.dumps(
                        [
                            {
                                "path": item.path,
                                "hash": item.content_hash,
                                "length": item.content_length,
                                "source": item.source,
                                "filing_id": item.filing_id,
                            }
                            for item in resolved_items
                        ],
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest(),
            )
            adoption = self.shadow_adopt(
                shadow_inventory,
                repository=repository,
                observed_at=observed_at,
            )
        return OrphanReconciliationReport(
            resolved_paths=tuple(item.path for item in resolved_items),
            skipped=dict(sorted(skipped.items())),
            shadow_adoption=adoption,
            network_requests=0,
        )

    def reconcile_shadow_adoption(
        self,
        inventory: ArchiveInventoryReport,
        *,
        repository: AnnouncementAssetRepository,
        legacy_catalog: Any | None = None,
        legacy_catalog_rows: Iterable[Mapping[str, Any]] = (),
        config: AnnouncementAssetConfig | None = None,
        config_fingerprint: str | None = None,
        legacy_custody_evidence_by_path: Mapping[
            str | Path, Mapping[str, Any]
        ] | None = None,
        gate_ttl_seconds: int = 24 * 60 * 60,
    ) -> ArchiveReconciliationReport:
        """Compare shared period winners with legacy active catalog decisions."""

        if not str(inventory.inventory_fingerprint or "").strip():
            raise ValueError("shadow adoption reconciliation requires inventory fingerprint")
        if config is None:
            raise ValueError(
                "shadow adoption reconciliation requires active configuration"
            )
        active_config_fingerprint = config.config_fingerprint
        if (
            config_fingerprint is not None
            and str(config_fingerprint) != active_config_fingerprint
        ):
            raise ValueError("shadow adoption configuration fingerprint mismatch")
        controlled_roots = (config.blob_root.resolve(strict=False),)
        ContentAddressedBlobStore(config).validate_mount()
        if not active_config_fingerprint:
            raise ValueError("shadow adoption reconciliation requires config fingerprint")
        if int(gate_ttl_seconds) <= 0:
            raise ValueError("adoption promotion gate TTL must be positive")
        if not controlled_roots:
            raise ValueError("shadow adoption reconciliation requires custody roots")
        legacy_custody = {
            str(Path(path).resolve(strict=False)): dict(evidence)
            for path, evidence in (legacy_custody_evidence_by_path or {}).items()
        }
        valid_inventory_items = tuple(
            item
            for item in inventory.items
            if item.status in {"adoptable", "duplicate", "superseded"}
            and item.content_hash
            and item.instrument_id
            and item.fiscal_year is not None
        )
        legacy_paths = {
            str(Path(item.path).resolve(strict=False)): item
            for item in valid_inventory_items
        }

        rows = [dict(row) for row in legacy_catalog_rows]
        if legacy_catalog is not None:
            rows.extend(
                dict(row)
                for row in legacy_catalog.list_assets(
                    active_only=True,
                    validate_files=True,
                )
            )
        legacy_by_period: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(
            list
        )
        for row in rows:
            instrument_id = _text(row.get("instrument_id"))
            period = _text(row.get("report_period"))
            if not instrument_id or not period or len(period) < 4:
                continue
            if row.get("is_active") is False:
                continue
            legacy_by_period[(normalize_instrument_id(instrument_id), int(period[:4]))].append(
                row
            )

        inventory_scopes = {
            (str(item.instrument_id), int(item.fiscal_year))
            for item in inventory.items
            if item.instrument_id
            and item.fiscal_year is not None
            and item.report_type in {"annual_report", "annual_report_correction"}
        }
        target_scopes = inventory_scopes | set(legacy_by_period)
        canonical_by_period: dict[tuple[str, int], EffectiveAnnualReport] = {}
        offset = 0
        while True:
            page = repository.list_effective_reports(
                include_shadow=True,
                limit=1000,
                offset=offset,
            )
            if not page:
                break
            canonical_by_period.update(
                {
                    (asset.instrument_id, asset.fiscal_year): asset
                    for asset in page
                    if (asset.instrument_id, asset.fiscal_year) in target_scopes
                }
            )
            offset += len(page)
        conflict_scopes = {
            (str(item.instrument_id), int(item.fiscal_year))
            for item in inventory.items
            if item.status == "conflicting"
            and item.instrument_id
            and item.fiscal_year is not None
        }
        periods: list[ArchivePeriodReconciliation] = []
        for scope in sorted(target_scopes):
            instrument_id, fiscal_year = scope
            canonical = canonical_by_period.get(scope)
            legacy = legacy_by_period.get(scope, [])
            candidate_rows = repository.list_candidate_rows(
                instrument_id=instrument_id,
                fiscal_year=fiscal_year,
                include_shadow=True,
            )
            registered_signatures = {
                (
                    _text(row.get("source")),
                    _text(row.get("source_announcement_id")),
                    _text(row.get("content_hash")),
                )
                for row in candidate_rows
            }
            expected_signatures = {
                (item.source, item.filing_id, item.content_hash)
                for item in inventory.items
                if item.instrument_id == instrument_id
                and item.fiscal_year == fiscal_year
                and item.status in {"adoptable", "duplicate", "superseded"}
            }
            legacy_ids = tuple(
                sorted(
                    str(row.get("source_file_id"))
                    for row in legacy
                    if row.get("source_file_id")
                )
            )
            if scope in conflict_scopes:
                status, reason = "conflicting", "legacy_inventory_conflict"
            elif not expected_signatures.issubset(registered_signatures):
                status, reason = "conflicting", "valid_manifest_not_adopted"
            elif canonical is None:
                status, reason = "conflicting", "canonical_winner_missing"
            elif canonical.decision_state in {
                EffectiveDecisionState.AMBIGUOUS,
                EffectiveDecisionState.BLOCKED,
                EffectiveDecisionState.PROVISIONAL,
                EffectiveDecisionState.WITHDRAWN,
            }:
                status, reason = "conflicting", "canonical_decision_not_ready"
            elif not legacy:
                status, reason = "canonical_only", "no_legacy_active_catalog_row"
            elif any(
                str(row.get("integrity_status") or "valid")
                not in {"valid", "unchecked"}
                for row in legacy
            ):
                status, reason = "conflicting", "legacy_catalog_integrity_failure"
            elif all(_catalog_row_matches_asset(row, canonical) for row in legacy):
                status, reason = "matched", "legacy_and_shared_winner_match"
            else:
                status, reason = "conflicting", "legacy_winner_mismatch"
            gate_id = None
            if status != "conflicting" and canonical is not None:
                blob = (
                    None
                    if not canonical.content_hash
                    else repository.get_blob(canonical.content_hash)
                )
                if blob is None:
                    status, reason = "conflicting", "canonical_blob_missing"
                else:
                    path = Path(blob.canonical_path).resolve(strict=False)
                    legacy_item = legacy_paths.get(str(path))
                    custody_root = (
                        path.parent
                        if legacy_item is not None
                        else next(
                            (
                                root
                                for root in controlled_roots
                                if _path_is_beneath_any(path, (root,))
                            ),
                            None,
                        )
                    )
                    if custody_root is None:
                        status, reason = "conflicting", "shared_custody_not_proven"
                    else:
                        try:
                            _verify_legacy_pdf(path, str(blob.content_hash))
                            content_length = path.stat().st_size
                            if content_length != int(blob.content_length):
                                raise ValueError("legacy file length mismatch")
                            mount_key = probe_mount_identity(path).filesystem_key
                        except (OSError, ValueError) as exc:
                            status, reason = (
                                "conflicting",
                                f"promotion_integrity_failure:{type(exc).__name__}",
                            )
                        else:
                            reconciled_at = utc_now_iso()
                            expires_at = (
                                datetime.fromisoformat(
                                    reconciled_at.replace("Z", "+00:00")
                                ).astimezone(timezone.utc)
                                + timedelta(seconds=int(gate_ttl_seconds))
                            ).isoformat()
                            gate_id = stable_id(
                                "adoption-promotion-gate",
                                canonical.asset_id,
                                str(inventory.inventory_fingerprint),
                                active_config_fingerprint,
                                str(blob.content_hash),
                                str(path),
                                mount_key,
                                reconciled_at,
                            )
                            custody_state = "canonical"
                            if legacy_item is not None:
                                custody_evidence = legacy_custody.get(str(path))
                                if not _valid_controlled_legacy_custody_evidence(
                                    custody_evidence,
                                    path=path,
                                    content_hash=str(blob.content_hash),
                                    mount_filesystem_key=mount_key,
                                    config_fingerprint=active_config_fingerprint,
                                ):
                                    status, reason = (
                                        "custody_pending",
                                        "legacy_shared_custody_not_proven",
                                    )
                                    gate_id = None
                                    for item in valid_inventory_items:
                                        if (
                                            item.instrument_id == instrument_id
                                            and item.fiscal_year == fiscal_year
                                            and item.content_hash == blob.content_hash
                                        ):
                                            repository.upsert_legacy_path_manifest(
                                                legacy_path=str(
                                                    Path(item.path).resolve(strict=False)
                                                ),
                                                consumer=item.consumer,
                                                asset_id=canonical.asset_id,
                                                content_hash=str(blob.content_hash),
                                                status="reconciled_pending_custody",
                                                manifest_version="legacy-rollback.v1",
                                                metadata={
                                                    "inventory_fingerprint": inventory.inventory_fingerprint,
                                                    "config_fingerprint": active_config_fingerprint,
                                                    "reconciliation_status": status,
                                                    "reconciliation_reason": reason,
                                                },
                                            )
                                    periods.append(
                                        ArchivePeriodReconciliation(
                                            instrument_id=instrument_id,
                                            fiscal_year=fiscal_year,
                                            status=status,
                                            reason=reason,
                                            canonical_asset_id=canonical.asset_id,
                                            canonical_content_hash=canonical.content_hash,
                                            legacy_source_file_ids=legacy_ids,
                                            promotion_gate_id=None,
                                        )
                                    )
                                    continue
                                custody_state = "shared_controlled_legacy"
                            repository.register_adoption_promotion_gate(
                                gate_id=gate_id,
                                asset_id=canonical.asset_id,
                                inventory_fingerprint=str(
                                    inventory.inventory_fingerprint
                                ),
                                config_fingerprint=active_config_fingerprint,
                                content_hash=str(blob.content_hash),
                                content_length=int(blob.content_length),
                                canonical_path=str(path),
                                mount_filesystem_key=mount_key,
                                custody_state=custody_state,
                                reconciled_at=reconciled_at,
                                expires_at=expires_at,
                                evidence={
                                    "custody_root": str(custody_root),
                                    "legacy_custody_evidence": legacy_custody.get(
                                        str(path)
                                    ),
                                    "legacy_source_file_ids": list(legacy_ids),
                                    "reconciliation_status": status,
                                    "reconciliation_reason": reason,
                                },
                            )
            periods.append(
                ArchivePeriodReconciliation(
                    instrument_id=instrument_id,
                    fiscal_year=fiscal_year,
                    status=status,
                    reason=reason,
                    canonical_asset_id=None if canonical is None else canonical.asset_id,
                    canonical_content_hash=(
                        None if canonical is None else canonical.content_hash
                    ),
                    legacy_source_file_ids=legacy_ids,
                    promotion_gate_id=gate_id,
                )
            )
        conflicts = sum(item.status == "conflicting" for item in periods)
        pending_custody = sum(item.status == "custody_pending" for item in periods)
        return ArchiveReconciliationReport(
            periods=tuple(periods),
            ready_for_cutover=conflicts == 0 and pending_custody == 0,
            conflict_count=conflicts,
            inventory_fingerprint=str(inventory.inventory_fingerprint),
            config_fingerprint=active_config_fingerprint,
        )

    def promote_shadow_adoption(
        self,
        reconciliation: ArchiveReconciliationReport,
        *,
        repository: AnnouncementAssetRepository,
        config: AnnouncementAssetConfig,
    ) -> tuple[EffectiveAnnualReport, ...]:
        """Promote reconciled assets independently from business consumer cutover."""
        if not reconciliation.ready_for_cutover:
            raise RuntimeError("shadow adoption reconciliation is not ready")
        if reconciliation.config_fingerprint != config.config_fingerprint:
            raise RuntimeError("shadow adoption configuration fingerprint changed")
        ContentAddressedBlobStore(config).validate_mount()
        promoted: list[EffectiveAnnualReport] = []
        for period in reconciliation.periods:
            if not period.canonical_asset_id:
                continue
            report = repository.get_effective_report_by_asset_id(
                period.canonical_asset_id,
                include_shadow=True,
            )
            if report is None:
                raise RuntimeError(
                    f"shadow asset missing during promotion: {period.canonical_asset_id}"
                )
            if not period.promotion_gate_id:
                raise RuntimeError("shadow asset has no persisted promotion gate")
            gate = repository.get_adoption_promotion_gate(period.promotion_gate_id)
            if gate is None:
                raise RuntimeError("shadow asset promotion gate is missing")
            path = Path(str(gate["canonical_path"])).resolve(strict=False)
            if gate["custody_state"] == "canonical":
                if not _path_is_beneath_any(
                    path, (config.blob_root.resolve(strict=False),)
                ):
                    raise RuntimeError("canonical adoption path escapes configured blob root")
            elif gate["custody_state"] == "shared_controlled_legacy":
                if not _path_is_beneath_any(path, config.adoption_roots):
                    raise RuntimeError("controlled legacy path escapes adoption roots")
                if not _valid_controlled_legacy_custody_evidence(
                    gate.get("evidence", {}).get("legacy_custody_evidence"),
                    path=path,
                    content_hash=str(gate["content_hash"]),
                    mount_filesystem_key=str(gate["mount_filesystem_key"]),
                    config_fingerprint=config.config_fingerprint,
                ):
                    raise RuntimeError("controlled legacy custody evidence is invalid")
            else:
                raise RuntimeError("unsupported adoption custody state")
            try:
                _verify_legacy_pdf(path, str(gate["content_hash"]))
                if path.stat().st_size != int(gate["content_length"]):
                    raise ValueError("legacy file length mismatch")
                mount_key = probe_mount_identity(path).filesystem_key
            except (OSError, ValueError) as exc:
                reason = f"promotion_integrity_failure:{type(exc).__name__}"
                repository.invalidate_adoption_promotion_gate(
                    period.promotion_gate_id,
                    reason=reason,
                )
                repository.mark_effective_content_invalid(
                    report.asset_id,
                    integrity_status=(
                        IntegrityStatus.MISSING
                        if isinstance(exc, FileNotFoundError)
                        else IntegrityStatus.HASH_MISMATCH
                    ),
                    reason=reason,
                )
                raise RuntimeError(reason) from exc
            promoted.append(
                repository.promote_effective_report(
                    report.asset_id,
                    promotion_gate_id=period.promotion_gate_id,
                    inventory_fingerprint=reconciliation.inventory_fingerprint,
                    config_fingerprint=config.config_fingerprint,
                    validated_mount_filesystem_key=mount_key,
                )
            )
        return tuple(promoted)

    def converge(
        self,
        inventory: ArchiveInventoryReport,
        *,
        repository: AnnouncementAssetRepository,
        config: AnnouncementAssetConfig,
        canonical_root: str | Path | None = None,
        dry_run: bool = True,
        approved_paths: Iterable[str | Path] = (),
        approved_plan_fingerprint: str | None = None,
        primary_failure_domain: str | None = None,
        alias_expires_at: str | None = None,
        operator_authorized: bool = False,
        use_hardlinks: bool = True,
        manifest_version: str = "legacy-rollback.v1",
        capability: NfsCapabilityProbe | None = None,
    ) -> ArchiveConvergenceReport:
        """Plan or execute verified legacy convergence with explicit gates."""
        root = config.blob_root.resolve(strict=False)
        if canonical_root is not None and Path(canonical_root).resolve(strict=False) != root:
            raise ValueError("canonical root does not match active configuration")
        ContentAddressedBlobStore(config).validate_mount()
        approved = {str(Path(path).resolve(strict=False)) for path in approved_paths}
        entries: list[Mapping[str, Any]] = []
        rollback: list[Mapping[str, Any]] = []
        excluded = 0
        for item in sorted(inventory.items, key=lambda value: value.path):
            legacy = Path(item.path).resolve(strict=False)
            if item.status not in {"adoptable", "duplicate", "superseded"}:
                excluded += 1
                entries.append(
                    {
                        "legacy_path": str(legacy),
                        "status": "excluded",
                        "reason": f"status:{item.status}",
                    }
                )
                continue
            if not item.content_hash or not item.instrument_id or item.fiscal_year is None:
                excluded += 1
                entries.append(
                    {
                        "legacy_path": str(legacy),
                        "status": "excluded",
                        "reason": "missing_verified_identity",
                    }
                )
                continue
            effective = repository.get_effective_report(
                item.instrument_id, int(item.fiscal_year), include_shadow=True
            )
            blob = repository.get_blob(str(item.content_hash))
            if effective is None or blob is None:
                excluded += 1
                entries.append(
                    {
                        "legacy_path": str(legacy),
                        "status": "excluded",
                        "reason": "canonical_effective_or_blob_missing",
                        "content_hash": item.content_hash,
                    }
                )
                continue
            if effective.content_hash != str(item.content_hash):
                excluded += 1
                entries.append(
                    {
                        "legacy_path": str(legacy),
                        "status": "excluded",
                        "reason": "superseded_requires_lifecycle_adoption",
                        "content_hash": item.content_hash,
                    }
                )
                continue
            digest = validate_path_segment(
                item.content_hash,
                kind="sha256",
                field_name="inventory.content_hash",
            )
            target = root / digest[:2] / f"{digest}.pdf"
            pending_reconciliation = repository.get_legacy_path_manifest(str(legacy))
            backup_state = repository.get_backup_state(digest)
            rollback_entry = {
                "legacy_path": str(legacy),
                "consumer": item.consumer,
                "asset_id": effective.asset_id,
                "content_hash": str(item.content_hash),
                "manifest_version": manifest_version,
                "target_path": str(target),
                "inventory_fingerprint": inventory.inventory_fingerprint,
                "config_fingerprint": config.config_fingerprint,
            }
            rollback.append(rollback_entry)
            approved_item = str(legacy) in approved
            entries.append(
                {
                    **rollback_entry,
                    "status": "approved" if approved_item else "candidate",
                    "action": "canonicalize" if approved_item else "excluded_not_allowlisted",
                    "reason": "approved_verified_legacy_asset"
                    if approved_item
                    else "requires_explicit_allowlist",
                    "reconciliation_evidence": _legacy_reconciliation_plan_projection(
                        pending_reconciliation
                    ),
                    "backup_evidence": _backup_state_plan_projection(backup_state),
                }
            )

        capability_plan = _capability_plan_projection(capability)
        plan_fingerprint = hashlib.sha256(
            canonical_json(
                {
                    "schema_version": "legacy_convergence_plan.v1",
                    "inventory_fingerprint": inventory.inventory_fingerprint,
                    "config_fingerprint": config.config_fingerprint,
                    "manifest_version": manifest_version,
                    "canonical_root": str(root),
                    "primary_failure_domain": str(primary_failure_domain or ""),
                    "alias_expires_at": str(alias_expires_at or ""),
                    "use_hardlinks": bool(use_hardlinks),
                    "capability": capability_plan,
                    "entries": entries,
                }
            ).encode("utf-8")
        ).hexdigest()
        if dry_run:
            return ArchiveConvergenceReport(
                dry_run=True,
                manifest_version=manifest_version,
                entries=tuple(entries),
                rollback_manifest=tuple(rollback),
                excluded_count=excluded,
                plan_fingerprint=plan_fingerprint,
            )
        if not operator_authorized:
            raise PermissionError("convergence requires explicit operator authorization")
        if str(approved_plan_fingerprint or "") != plan_fingerprint:
            raise RuntimeError("convergence plan fingerprint is missing or stale")
        if not str(primary_failure_domain or "").strip():
            raise RuntimeError("convergence requires a primary failure domain")
        _require_future_timestamp(alias_expires_at, field_name="managed alias expiry")
        if capability is None or not capability.probed:
            raise RuntimeError("convergence requires a completed filesystem capability probe")
        if capability.target_root != str(root):
            raise RuntimeError("convergence capability target does not match configuration")

        copied = linked = deleted = 0
        for entry in entries:
            if entry.get("status") != "approved":
                continue
            legacy = Path(str(entry["legacy_path"]))
            target = Path(str(entry["target_path"]))
            digest = validate_path_segment(
                entry["content_hash"],
                kind="sha256",
                field_name="convergence.content_hash",
            )
            effective = repository.get_effective_report_by_asset_id(
                str(entry["asset_id"]), include_shadow=True
            )
            if (
                effective is None
                or effective.availability is not AssetAvailability.LOCAL_VALID
                or effective.content_hash != digest
                or effective.decision_state in {
                    EffectiveDecisionState.AMBIGUOUS,
                    EffectiveDecisionState.BLOCKED,
                    EffectiveDecisionState.PROVISIONAL,
                    EffectiveDecisionState.WITHDRAWN,
                }
            ):
                raise RuntimeError("shared adoption read evidence is not ready")
            reconciliation_evidence = repository.get_legacy_path_manifest(str(legacy))
            if _legacy_reconciliation_plan_projection(reconciliation_evidence) != entry.get(
                "reconciliation_evidence"
            ):
                raise RuntimeError("legacy reconciliation evidence changed after planning")
            if not _legacy_reconciliation_evidence_matches(
                reconciliation_evidence,
                legacy_path=str(legacy),
                asset_id=effective.asset_id,
                content_hash=digest,
                inventory_fingerprint=inventory.inventory_fingerprint,
                config_fingerprint=config.config_fingerprint,
            ):
                raise RuntimeError("legacy path has no exact persisted reconciliation")
            blob = repository.get_blob(digest)
            if blob is None or Path(blob.canonical_path).resolve(strict=False) not in {
                legacy.resolve(strict=False),
                target.resolve(strict=False),
            }:
                raise RuntimeError("canonical blob reference changed after planning")
            legacy_pin = repository.get_active_retention_pin(
                blob_hash=digest,
                pin_type="legacy_alias",
                pin_key=str(legacy),
            )
            if legacy_pin is None or legacy_pin.get("owner") != entry["consumer"]:
                raise RuntimeError("legacy path has no exact active database retention pin")
            backup = repository.get_backup_state(digest)
            if _backup_state_plan_projection(backup) != entry.get("backup_evidence"):
                raise RuntimeError("backup evidence changed after planning")
            _validate_convergence_backup_state(
                backup,
                config=config,
                content_hash=digest,
                content_length=int(blob.content_length),
                primary_failure_domain=str(primary_failure_domain),
            )
            assert backup is not None
            backup_object = Path(str(backup["backup_path"])).resolve(strict=False)
            backup_failure_domain = str(backup["failure_domain"])
            file_manifest_watermark = str(backup["file_manifest_watermark"])
            backup_verified_at = str(backup["verified_at"])
            _verify_legacy_pdf(backup_object, digest)
            if backup_object.stat().st_size != int(blob.content_length):
                raise RuntimeError("backup object length does not match legacy file")
            backup_mount_key = probe_mount_identity(backup_object).filesystem_key
            if backup_mount_key != str(backup["destination_identity"]):
                raise RuntimeError("backup mount identity changed")
            if backup_mount_key in {
                capability.source_filesystem_key,
                capability.target_filesystem_key,
            }:
                raise RuntimeError("backup shares the primary storage failure domain")
            _revalidate_convergence_mounts(
                legacy=legacy,
                root=root,
                backup_object=backup_object,
                capability=capability,
                backup_mount_key=backup_mount_key,
                config=config,
            )
            _verify_legacy_pdf(legacy, digest)
            if legacy.stat().st_size != int(blob.content_length):
                raise RuntimeError("legacy file length changed after planning")
            _require_safe_canonical_target(root, target)
            _revalidate_convergence_mounts(
                legacy=legacy,
                root=root,
                backup_object=backup_object,
                capability=capability,
                backup_mount_key=backup_mount_key,
                config=config,
            )
            revalidate = lambda legacy=legacy, backup_object=backup_object, backup_mount_key=backup_mount_key: _revalidate_convergence_mounts(
                legacy=legacy,
                root=root,
                backup_object=backup_object,
                capability=capability,
                backup_mount_key=backup_mount_key,
                config=config,
            )
            parent_existed = target.parent.exists()
            target_created = False
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                _require_safe_canonical_target(root, target)
                revalidate()
                if not target.exists():
                    if use_hardlinks and capability.can_hardlink:
                        os.link(legacy, target)
                        target_created = True
                        linked += 1
                    else:
                        if not capability.can_atomic_rename:
                            raise RuntimeError(
                                "convergence copy requires verified atomic rename support"
                            )
                        _verified_copy(
                            legacy,
                            target,
                            digest,
                            before_publish=revalidate,
                        )
                        target_created = True
                        copied += 1
                else:
                    if target.is_symlink():
                        raise RuntimeError("canonical target must not be a symlink")
                    _verify_legacy_pdf(target, digest)
                _verify_legacy_pdf(target, digest)
                if target.stat().st_size != legacy.stat().st_size:
                    raise RuntimeError("canonical target length verification failed")
                revalidate()
            except ConvergenceMountRaceError:
                if target_created:
                    target.unlink(missing_ok=True)
                if not parent_existed:
                    _remove_empty_convergence_parents(target.parent, stop=root)
                raise
            recovery_pair_id = stable_id(
                "legacy-path-recovery-pair",
                plan_fingerprint,
                str(legacy),
                digest,
            )
            recovery_id = stable_id(
                "legacy-path-rollback",
                recovery_pair_id,
                str(entry["asset_id"]),
            )
            recovery_entry = OfficialAssetRecoveryManifestEntry(
                recovery_id=recovery_id,
                manifest_kind="legacy_path_rollback",
                manifest_version=1,
                predecessor_asset_id=str(entry["asset_id"]),
                source=effective.source,
                source_announcement_id=effective.source_announcement_id,
                attachment_id=effective.attachment_id,
                version_id=effective.version_id,
                prior_path=str(legacy),
                content_hash=digest,
                replacement_asset_id=effective.asset_id,
                replacement_content_hash=digest,
                backup_object=str(backup_object),
                file_manifest_watermark=file_manifest_watermark,
                recovery_pair_id=recovery_pair_id,
                consumer=str(entry["consumer"]),
                active_indefinitely=True,
                created_at=backup_verified_at,
                created_by="announcement_archive_convergence",
                evidence={
                    "plan_fingerprint": plan_fingerprint,
                    "inventory_fingerprint": inventory.inventory_fingerprint,
                    "canonical_target": str(target),
                    "backup_failure_domain": backup_failure_domain,
                    "backup_mount_filesystem_key": backup_mount_key,
                },
            )
            repository.finalize_legacy_path_convergence(
                content_hash=digest,
                content_length=int(blob.content_length),
                expected_blob_paths=(str(legacy), str(target)),
                canonical_path=str(target),
                legacy_path=str(legacy),
                consumer=str(entry["consumer"]),
                asset_id=str(entry["asset_id"]),
                alias_expires_at=str(alias_expires_at),
                alias_cutover_metadata={
                    "plan_fingerprint": plan_fingerprint,
                    "inventory_fingerprint": inventory.inventory_fingerprint,
                    "canonical_target": str(target),
                    "manifest_version": manifest_version,
                },
                recovery_entry=recovery_entry,
                legacy_manifest_version=manifest_version,
                legacy_manifest_metadata={
                    "target_path": str(target),
                    "plan_fingerprint": plan_fingerprint,
                    "recovery_id": recovery_id,
                    "recovery_pair_id": recovery_pair_id,
                },
            )
        return ArchiveConvergenceReport(
            dry_run=False,
            manifest_version=manifest_version,
            entries=tuple(entries),
            rollback_manifest=tuple(rollback),
            excluded_count=excluded,
            plan_fingerprint=plan_fingerprint,
            files_moved=0,
            files_copied=copied,
            files_linked=linked,
            files_deleted=deleted,
        )

    @staticmethod
    def _mark_existing_effective_unavailable(
        repository: AnnouncementAssetRepository,
        *,
        instrument_id: str,
        fiscal_year: int,
        decision_state: EffectiveDecisionState,
        reason: str,
        timestamp: str,
    ) -> None:
        existing = repository.get_effective_report(
            instrument_id,
            fiscal_year,
            include_shadow=True,
        )
        if existing is None:
            return
        repository.upsert_effective_report(
            replace(
                existing,
                decision_state=decision_state,
                availability=(
                    AssetAvailability.AMBIGUOUS
                    if decision_state is EffectiveDecisionState.AMBIGUOUS
                    else AssetAvailability.BLOCKED
                ),
                last_checked_at=timestamp,
                decision_reasons=(reason,),
            )
        )

    def _inspect_file(
        self,
        path: Path,
        *,
        consumer: str,
        manifests: list[dict[str, Any]],
    ) -> ArchiveInventoryItem:
        if "derived" in path.parts or path.suffix.lower() != ".pdf":
            return ArchiveInventoryItem(
                path=str(path),
                consumer=consumer,
                status="derived",
                reason="derived_or_non_pdf_file",
            )
        parsed = self._parse_path(path, consumer=consumer)
        if parsed is None:
            return ArchiveInventoryItem(
                path=str(path),
                consumer=consumer,
                status="orphan",
                reason="unrecognized_legacy_filename",
            )
        if parsed["report_type"] != "annual_report":
            return ArchiveInventoryItem(
                path=str(path),
                consumer=consumer,
                status="out_of_scope",
                reason="non_annual_period",
                **parsed,
            )
        if len(manifests) != 1:
            return ArchiveInventoryItem(
                path=str(path),
                consumer=consumer,
                status="orphan" if not manifests else "conflicting",
                reason=(
                    "manifest_missing"
                    if not manifests
                    else "multiple_manifests_for_path"
                ),
                **parsed,
            )
        manifest = manifests[0]
        manifest_type = _normalize_report_type(manifest.get("report_type"))
        if (
            manifest_type
            and normalize_document_family(manifest_type)
            != DocumentFamily.ANNUAL_REPORT.value
        ):
            return ArchiveInventoryItem(
                path=str(path),
                consumer=consumer,
                status="out_of_scope",
                reason="non_annual_document_family",
                source=_text(manifest.get("source")),
                source_file_id=_text(manifest.get("source_file_id")),
                manifest=manifest,
                **parsed,
            )
        evidence_error = self._manifest_identity_error(parsed, manifest)
        if evidence_error:
            return ArchiveInventoryItem(
                path=str(path),
                consumer=consumer,
                status="conflicting",
                reason=evidence_error,
                source=_text(manifest.get("source")),
                source_file_id=_text(manifest.get("source_file_id")),
                manifest=manifest,
                **parsed,
            )
        if manifest_type:
            parsed["report_type"] = manifest_type
        try:
            stat = path.stat()
            with path.open("rb") as handle:
                if handle.read(5) != b"%PDF-":
                    return ArchiveInventoryItem(
                        path=str(path),
                        consumer=consumer,
                        status="corrupt",
                        reason="invalid_pdf_signature",
                        source=_text(manifest.get("source")),
                        source_file_id=_text(manifest.get("source_file_id")),
                        manifest=manifest,
                        **parsed,
                    )
                handle.seek(0)
                digest = hashlib.sha256()
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            actual_hash = digest.hexdigest()
        except OSError as exc:
            return ArchiveInventoryItem(
                path=str(path),
                consumer=consumer,
                status="corrupt",
                reason=f"unreadable:{type(exc).__name__}",
                source=_text(manifest.get("source")),
                source_file_id=_text(manifest.get("source_file_id")),
                manifest=manifest,
                **parsed,
            )
        expected_hash = _text(manifest.get("content_hash")) or parsed.get(
            "expected_hash"
        )
        expected_length = manifest.get("content_length")
        if expected_length is not None and int(expected_length) != stat.st_size:
            return ArchiveInventoryItem(
                path=str(path),
                consumer=consumer,
                status="corrupt",
                reason="content_length_mismatch",
                content_hash=actual_hash,
                content_length=stat.st_size,
                source=_text(manifest.get("source")),
                source_file_id=_text(manifest.get("source_file_id")),
                manifest=manifest,
                **parsed,
            )
        if expected_hash and actual_hash != expected_hash.lower():
            return ArchiveInventoryItem(
                path=str(path),
                consumer=consumer,
                status="corrupt",
                reason="content_hash_mismatch",
                content_hash=actual_hash,
                content_length=stat.st_size,
                source=_text(manifest.get("source")),
                source_file_id=_text(manifest.get("source_file_id")),
                manifest=manifest,
                **parsed,
            )
        return ArchiveInventoryItem(
            path=str(path),
            consumer=consumer,
            status="adoptable",
            reason="verified_manifest_and_file",
            source=normalize_source(str(manifest.get("source") or "")),
            content_hash=actual_hash,
            content_length=stat.st_size,
            source_file_id=_text(manifest.get("source_file_id")),
            manifest=manifest,
            **parsed,
        )

    @staticmethod
    def _parse_path(path: Path, *, consumer: str) -> dict[str, Any] | None:
        pattern = (
            _BUSINESS_FILENAME
            if consumer == "business_profile"
            else _BROKER_FILENAME
        )
        match = pattern.match(path.name)
        if match is None:
            return None
        values = match.groupdict()
        if consumer == "business_profile":
            period_label = values["period"]
            year = int(period_label[:4])
            quarter = int(period_label[-1])
            report_period = f"{year}-{'12-31' if quarter == 4 else '06-30' if quarter == 2 else '03-31' if quarter == 1 else '09-30'}"
            suffix = values["suffix"]
            expected_hash = values["digest"].lower()
        else:
            report_period = values["period"]
            year = int(report_period[:4])
            exchange = next(
                (
                    part.upper()
                    for part in path.parts
                    if part.upper() in _EXCHANGE_SUFFIX
                ),
                None,
            )
            if exchange is None:
                return None
            suffix = _EXCHANGE_SUFFIX[exchange]
            expected_hash = None
        return {
            "instrument_id": normalize_instrument_id(
                f"{values['symbol']}.{suffix}"
            ),
            "exchange": {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}[suffix],
            "report_period": report_period,
            "fiscal_year": year,
            "report_type": (
                "annual_report" if report_period.endswith("12-31") else "semiannual"
            ),
            "filing_id": values["filing"],
            "expected_hash": expected_hash,
        }

    @staticmethod
    def _manifest_identity_error(
        parsed: Mapping[str, Any], manifest: Mapping[str, Any]
    ) -> str | None:
        comparisons = {
            "instrument_id": normalize_instrument_id(
                str(manifest.get("instrument_id") or parsed["instrument_id"])
            ),
            "exchange": str(manifest.get("exchange") or parsed["exchange"]).upper(),
            "report_period": str(
                manifest.get("report_period") or parsed["report_period"]
            ),
            "filing_id": str(manifest.get("filing_id") or parsed["filing_id"]),
        }
        for field_name, value in comparisons.items():
            if value != parsed[field_name]:
                return f"manifest_{field_name}_mismatch"
        if not _text(manifest.get("source")):
            return "manifest_source_missing"
        manifest_type = _normalize_report_type(manifest.get("report_type"))
        parsed_type = str(parsed.get("report_type") or "")
        if manifest_type and (
            manifest_type == "semiannual" and parsed_type != "semiannual"
            or manifest_type in {"annual_report", "annual_report_correction"}
            and parsed_type == "semiannual"
        ):
            return "manifest_report_type_mismatch"
        return None

    @staticmethod
    def _missing_manifest_item(
        path: Path, manifest: Mapping[str, Any]
    ) -> ArchiveInventoryItem:
        return ArchiveInventoryItem(
            path=str(path),
            consumer=str(
                (manifest.get("metadata") or {}).get("consumer") or "manifest"
            ),
            status="missing",
            reason="manifest_path_missing",
            instrument_id=_text(manifest.get("instrument_id")),
            exchange=_text(manifest.get("exchange")),
            report_period=_text(manifest.get("report_period")),
            fiscal_year=(
                int(str(manifest["report_period"])[:4])
                if manifest.get("report_period")
                else None
            ),
            report_type=_normalize_report_type(manifest.get("report_type")),
            source=_text(manifest.get("source")),
            filing_id=_text(manifest.get("filing_id")),
            expected_hash=_text(manifest.get("content_hash")),
            source_file_id=_text(manifest.get("source_file_id")),
            manifest=dict(manifest),
        )

    @staticmethod
    def _exclude_explicit_summaries(
        items: list[ArchiveInventoryItem],
    ) -> list[ArchiveInventoryItem]:
        """Remove only manifest candidates whose own PDF proves it is a summary."""

        output = list(items)
        for index, item in enumerate(items):
            if (
                item.status in {"adoptable", "superseded"}
                and item.document_family == DocumentFamily.ANNUAL_REPORT.value
                and item.is_full_report
                and _pdf_first_page_is_explicit_annual_report_summary(Path(item.path))
            ):
                output[index] = replace(
                    item,
                    status="out_of_scope",
                    reason="verified_pdf_summary_title",
                )
        return output

    @staticmethod
    def _apply_duplicate_and_conflict_status(
        items: list[ArchiveInventoryItem],
    ) -> list[ArchiveInventoryItem]:
        legal_groups: dict[tuple[str, str], list[int]] = defaultdict(list)
        hash_groups: dict[str, list[int]] = defaultdict(list)
        for index, item in enumerate(items):
            if item.status != "adoptable":
                continue
            if item.legal_key:
                legal_groups[item.legal_key].append(index)
            if item.content_hash:
                hash_groups[item.content_hash].append(index)
        output = list(items)
        for indexes in legal_groups.values():
            hashes = {items[index].content_hash for index in indexes}
            scopes = {
                (
                    items[index].instrument_id,
                    items[index].fiscal_year,
                    items[index].report_period,
                )
                for index in indexes
            }
            if len(hashes) > 1 or len(scopes) > 1:
                for index in indexes:
                    output[index] = replace(
                        items[index],
                        status="conflicting",
                        reason=(
                            "same_legal_filing_has_different_content"
                            if len(hashes) > 1
                            else "same_legal_filing_has_different_identity"
                        ),
                    )
        for indexes in hash_groups.values():
            adoptable_indexes = [
                index for index in indexes if output[index].status == "adoptable"
            ]
            if len(adoptable_indexes) > 1:
                for index in adoptable_indexes:
                    output[index] = replace(
                        output[index],
                        status="duplicate",
                        reason="verified_duplicate_content",
                    )
        return output


def _text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _pdf_first_page_is_explicit_annual_report_summary(path: Path) -> bool:
    """Exclude only a PDF whose first-page text explicitly says annual summary."""

    try:
        from pypdf import PdfReader
        from pypdf.errors import PyPdfError

        reader = PdfReader(str(path), strict=False)
        if not reader.pages:
            return False
        text = re.sub(r"\s+", "", str(reader.pages[0].extract_text() or ""))
    except (OSError, ValueError, TypeError, IndexError, KeyError, PyPdfError):
        # Legacy inventory remains fail-closed on identity conflicts when the
        # PDF cannot provide definitive title evidence.
        return False
    return bool(re.search(r"\d{4}年年度报告摘要", text))


def _path_is_beneath_any(path: Path, roots: tuple[Path, ...]) -> bool:
    resolved = path.resolve(strict=False)
    for root in roots:
        try:
            resolved.relative_to(root.resolve(strict=False))
        except ValueError:
            continue
        return True
    return False


def _registered_template_values(
    path: Path,
    *,
    consumer: str,
    registry: LegacyArchiveRegistryConfig,
) -> dict[str, str] | None:
    template = (
        registry.business_profile_template
        if consumer == "business_profile"
        else registry.broker_risk_control_template
    ).strip("/")
    base_root = (
        registry.business_profile_root
        if consumer == "business_profile"
        else registry.broker_risk_control_root
    )
    try:
        directory_below_root = path.parent.resolve(strict=False).relative_to(
            Path(base_root).resolve(strict=False)
        ).as_posix()
    except ValueError:
        return None
    relative_directory = f"{consumer}/{directory_below_root}"
    expression = re.escape(template)
    replacements = {
        re.escape("{fiscal_year}"): r"(?P<fiscal_year>\d{4})",
        re.escape("{exchange}"): r"(?P<exchange>SSE|SZSE|BSE)",
        re.escape("{symbol}"): r"(?P<symbol>\d{6})",
    }
    for placeholder, pattern in replacements.items():
        expression = expression.replace(placeholder, pattern)
    match = re.fullmatch(expression, relative_directory)
    return None if match is None else match.groupdict()


def _registered_template_identity_error(
    template_values: Mapping[str, str],
    *,
    parsed: Mapping[str, Any] | None,
    manifests: Sequence[Mapping[str, Any]],
    consumer: str,
) -> str | None:
    if parsed is None:
        return None
    captured_year = template_values.get("fiscal_year")
    if captured_year is not None and int(captured_year) != int(parsed["fiscal_year"]):
        return "template_fiscal_year_mismatch"
    captured_exchange = template_values.get("exchange")
    if captured_exchange is not None and captured_exchange != parsed["exchange"]:
        return "template_exchange_mismatch"
    captured_symbol = template_values.get("symbol")
    parsed_symbol = str(parsed["instrument_id"]).split(".", 1)[0]
    if captured_symbol is not None and captured_symbol != parsed_symbol:
        return "template_symbol_mismatch"
    if len(manifests) != 1:
        return None
    manifest = manifests[0]
    manifest_period = _text(manifest.get("report_period"))
    if (
        captured_year is not None
        and manifest_period is not None
        and len(manifest_period) >= 4
        and captured_year != manifest_period[:4]
    ):
        return "template_fiscal_year_mismatch"
    manifest_exchange = _text(manifest.get("exchange"))
    if (
        captured_exchange is not None
        and manifest_exchange is not None
        and captured_exchange != manifest_exchange.upper()
    ):
        return "template_exchange_mismatch"
    manifest_instrument = _text(manifest.get("instrument_id"))
    if captured_symbol is not None and manifest_instrument is not None:
        try:
            manifest_symbol = normalize_instrument_id(manifest_instrument).split(
                ".", 1
            )[0]
        except (TypeError, ValueError):
            return None
        if captured_symbol != manifest_symbol:
            return "template_symbol_mismatch"
    if consumer not in {"business_profile", "broker_risk_control"}:
        return "path_outside_registered_template"
    return None


def _unknown_legacy_directories(
    filings_root: Path,
    *,
    registered_roots: tuple[Path, ...],
    known_roots: tuple[Path, ...],
) -> tuple[str, ...]:
    root = filings_root.resolve(strict=False)
    if not root.is_dir():
        return ()
    governed = tuple(
        item.resolve(strict=False) for item in (*registered_roots, *known_roots)
    )
    unknown: list[str] = []

    def visit(parent: Path) -> None:
        try:
            children = sorted(item for item in parent.iterdir() if item.is_dir())
        except OSError:
            return
        for child in children:
            resolved = child.resolve(strict=False)
            if any(resolved == item or _path_is_beneath_any(resolved, (item,)) for item in governed):
                continue
            if any(_path_is_beneath_any(item, (resolved,)) for item in governed):
                visit(resolved)
                continue
            unknown.append(str(resolved))

    visit(root)
    return tuple(sorted(unknown))


def _manifest_classification(manifest: Mapping[str, Any]) -> Mapping[str, Any]:
    direct = manifest.get("asset_classification")
    if isinstance(direct, Mapping):
        return direct
    metadata = manifest.get("metadata")
    if isinstance(metadata, Mapping):
        nested = metadata.get("asset_classification")
        if isinstance(nested, Mapping):
            return nested
    return {}


def _manifest_orphan_evidence(
    manifest: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    metadata = manifest.get("metadata")
    if not isinstance(metadata, Mapping):
        return None
    evidence = metadata.get("orphan_reconciliation")
    return evidence if isinstance(evidence, Mapping) else None


def _validate_orphan_evidence(
    mapping: Mapping[str, Any], *, evidence_kind: str
) -> str | None:
    required = (
        "source",
        "instrument_id",
        "exchange",
        "report_period",
        "fiscal_year",
        "source_announcement_id",
        "attachment_id",
        "content_hash",
        "content_length",
        "evidence_id",
    )
    missing = [name for name in required if not str(mapping.get(name) or "").strip()]
    if missing:
        return "identity_incomplete:" + ",".join(missing)
    if evidence_kind == "official_metadata":
        if str(mapping.get("evidence_type") or "official_metadata") not in {
            "official_metadata",
            "official_discovery",
        }:
            return "official_evidence_type_invalid"
        if mapping.get("metadata_only", True) is not True:
            return "official_evidence_not_metadata_only"
    else:
        audit_fields = ("audit_id", "audited_by", "audited_at")
        if any(not str(mapping.get(name) or "").strip() for name in audit_fields):
            return "operator_audit_incomplete"
        if str(mapping.get("evidence_type") or "audited_operator_mapping") != (
            "audited_operator_mapping"
        ):
            return "operator_evidence_type_invalid"
    evidence = mapping.get("evidence")
    if not isinstance(evidence, Mapping) or not evidence:
        return "evidence_not_verifiable"
    evidence_id = _text(mapping.get("evidence_id"))
    nested_evidence_id = _text(evidence.get("evidence_id"))
    if not evidence_id or nested_evidence_id != evidence_id:
        return "evidence_id_not_bound"
    evidence_identity = evidence.get("identity")
    if not isinstance(evidence_identity, Mapping):
        evidence_identity = evidence
    identity_fields = (
        "source",
        "source_announcement_id",
        "attachment_id",
        "instrument_id",
        "exchange",
        "report_period",
        "fiscal_year",
        "content_hash",
        "content_length",
    )
    for name in identity_fields:
        if not str(evidence_identity.get(name) or "").strip():
            return f"evidence_identity_incomplete:{name}"
        if name == "content_length":
            try:
                if int(evidence_identity[name]) != int(mapping[name]):
                    return "evidence_identity_mismatch:content_length"
            except (TypeError, ValueError):
                return "evidence_identity_invalid:content_length"
        elif name == "fiscal_year":
            try:
                if int(evidence_identity[name]) != int(mapping[name]):
                    return "evidence_identity_mismatch:fiscal_year"
            except (TypeError, ValueError):
                return "evidence_identity_invalid:fiscal_year"
        elif name == "content_hash":
            if str(evidence_identity[name]).lower() != str(mapping[name]).lower():
                return "evidence_identity_mismatch:content_hash"
        elif name == "source":
            if normalize_source(str(evidence_identity[name])) != normalize_source(
                str(mapping[name])
            ):
                return "evidence_identity_mismatch:source"
        elif name == "exchange":
            if str(evidence_identity[name]).upper() != str(mapping[name]).upper():
                return "evidence_identity_mismatch:exchange"
        elif str(evidence_identity[name]).strip() != str(mapping[name]).strip():
            return f"evidence_identity_mismatch:{name}"
    expected_evidence_hash = _orphan_evidence_hash(evidence)
    supplied_evidence_hash = _text(
        mapping.get("evidence_hash") or evidence.get("evidence_hash")
    )
    if evidence_kind == "official_metadata":
        if supplied_evidence_hash != expected_evidence_hash:
            return "official_evidence_hash_invalid"
    elif supplied_evidence_hash and supplied_evidence_hash != expected_evidence_hash:
        return "operator_evidence_hash_invalid"
    try:
        fiscal_year = int(mapping["fiscal_year"])
        if fiscal_year < 1990:
            return "fiscal_year_invalid"
        if int(mapping["content_length"]) <= 0:
            return "content_length_invalid"
        validate_path_segment(
            str(mapping["content_hash"]).lower(),
            kind="sha256",
            field_name="orphan.content_hash",
        )
        instrument_id = normalize_instrument_id(str(mapping["instrument_id"]))
    except (TypeError, ValueError):
        return "identity_value_invalid"
    report_period = str(mapping["report_period"]).strip()
    period_match = re.fullmatch(r"(\d{4})-\d{2}-\d{2}", report_period)
    if period_match is None:
        return "report_period_invalid"
    if int(period_match.group(1)) != fiscal_year:
        return "fiscal_year_report_period_mismatch"
    exchange = str(mapping["exchange"]).strip().upper()
    expected_suffix = _EXCHANGE_SUFFIX.get(exchange)
    if expected_suffix is None:
        return "exchange_invalid"
    instrument_parts = instrument_id.rsplit(".", 1)
    if len(instrument_parts) != 2 or instrument_parts[1] != expected_suffix:
        return "instrument_exchange_mismatch"
    if normalize_document_family(
        mapping.get("document_family") or "annual_report"
    ) != DocumentFamily.ANNUAL_REPORT.value:
        return "document_family_not_annual"
    if not str(mapping["report_period"]).endswith("-12-31"):
        return "report_period_not_annual"
    return None


def _orphan_evidence_hash(evidence: Mapping[str, Any]) -> str:
    """Hash provider/operator evidence without recursively hashing its digest."""

    payload = dict(evidence)
    payload.pop("evidence_hash", None)
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _orphan_evidence_record(
    mapping: Mapping[str, Any], *, evidence_kind: str
) -> dict[str, Any] | None:
    """Return the immutable evidence projection persisted in shadow metadata."""

    evidence = mapping.get("evidence")
    evidence_id = _text(mapping.get("evidence_id"))
    if not isinstance(evidence, Mapping) or not evidence_id:
        return None
    expected_hash = _orphan_evidence_hash(evidence)
    supplied_hash = _text(mapping.get("evidence_hash") or evidence.get("evidence_hash"))
    if evidence_kind == "official_metadata" and supplied_hash != expected_hash:
        return None
    if supplied_hash and supplied_hash != expected_hash:
        return None
    record: dict[str, Any] = {
        "evidence_kind": evidence_kind,
        "evidence_id": evidence_id,
        "evidence_hash": expected_hash,
        "evidence": dict(evidence),
    }
    if evidence_kind == "audited_operator_mapping":
        record.update(
            {
                "audit_id": str(mapping["audit_id"]),
                "audited_by": str(mapping["audited_by"]),
                "audited_at": str(mapping["audited_at"]),
            }
        )
    return record


def _orphan_classification(
    mapping: Mapping[str, Any],
) -> AnnualReportClassification:
    variant = normalize_annual_report_variant(
        mapping.get("variant") or mapping.get("report_type") or "original",
        correction_evidence=bool(mapping.get("correction_evidence")),
    )
    fiscal_year = int(mapping["fiscal_year"])
    return AnnualReportClassification(
        document_family=DocumentFamily.ANNUAL_REPORT.value,
        fiscal_year=fiscal_year,
        report_period=str(mapping["report_period"]),
        variant=variant,
        is_full_report=bool(mapping.get("is_full_report", True)),
        is_eligible=bool(mapping.get("is_full_report", True)),
        correction_evidence=variant is AnnualReportVariant.CORRECTION,
        reasons=("orphan_official_metadata_resolved",),
        policy_version=str(mapping.get("classifier_version") or "orphan_reconciliation.v1"),
    )


def _normalize_report_type(value: Any) -> str | None:
    report_type = str(value or "").strip().lower()
    if report_type in {"annual", "annual_report"}:
        return "annual_report"
    if report_type in {"annual_report_correction", "annual_correction", "correction"}:
        return "annual_report_correction"
    if report_type in {
        "correction_notice",
        "annual_report_notice",
        "annual_report_correction_notice",
    }:
        return "correction_notice"
    if report_type in {"semiannual", "semiannual_report", "half_year"}:
        return "semiannual"
    return report_type or None


def _published_at(item: ArchiveInventoryItem) -> str | None:
    return _text(
        item.manifest.get("published_at")
        or item.manifest.get("downloaded_at")
        or item.manifest.get("created_at")
    )


def _legacy_item_precedence_key(item: ArchiveInventoryItem) -> tuple[str, str, str]:
    return (
        _published_at(item) or "",
        item.source_file_id or "",
        item.path,
    )


def _explicit_legacy_supersedes_chain(
    rows: Sequence[tuple[AnnualReportCandidate, ArchiveInventoryItem, str]],
    *,
    winner_row: tuple[AnnualReportCandidate, ArchiveInventoryItem, str],
    scope_items: Sequence[ArchiveInventoryItem],
) -> tuple[
    tuple[tuple[AnnualReportCandidate, ArchiveInventoryItem, str], ...],
    str | None,
]:
    """Resolve one winner's explicit, same-source legal predecessor chain."""

    rows_by_candidate_id = {row[0].candidate_id: row for row in rows}
    candidate_by_legal_key = {
        row[1].legal_key: row[0].candidate_id
        for row in rows
        if row[1].legal_key is not None
    }
    aliases: dict[str, set[str]] = defaultdict(set)
    predecessor_refs: dict[str, set[str]] = defaultdict(set)
    for item in scope_items:
        candidate_id = candidate_by_legal_key.get(item.legal_key)
        if candidate_id is None:
            continue
        if item.source_file_id:
            aliases[str(item.source_file_id)].add(candidate_id)
        predecessor = _text(item.manifest.get("supersedes_source_file_id"))
        if predecessor:
            predecessor_refs[candidate_id].add(predecessor)

    current_id = winner_row[0].candidate_id
    newest_first: list[tuple[AnnualReportCandidate, ArchiveInventoryItem, str]] = []
    visited: set[str] = set()
    while True:
        if current_id in visited:
            return (), "legacy_supersedes_chain_cycle"
        visited.add(current_id)
        current_row = rows_by_candidate_id.get(current_id)
        if current_row is None:
            return (), "legacy_supersedes_chain_candidate_missing"
        newest_first.append(current_row)
        references = predecessor_refs.get(current_id, set())
        if len(references) > 1:
            return (), "legacy_supersedes_chain_conflicting_predecessors"
        if not references:
            break
        if (
            current_row[0].classification.variant
            is not AnnualReportVariant.CORRECTION
        ):
            return (), "legacy_supersedes_chain_non_correction_edge"
        predecessor_ref = next(iter(references))
        predecessor_candidates = aliases.get(predecessor_ref, set())
        if len(predecessor_candidates) != 1:
            return (), "legacy_supersedes_chain_predecessor_unresolved"
        predecessor_id = next(iter(predecessor_candidates))
        predecessor_row = rows_by_candidate_id.get(predecessor_id)
        if predecessor_row is None:
            return (), "legacy_supersedes_chain_predecessor_missing"
        predecessor_item = predecessor_row[1]
        current_item = current_row[1]
        if normalize_source(str(predecessor_item.source)) != normalize_source(
            str(current_item.source)
        ):
            return (), "legacy_supersedes_chain_cross_source"
        if (
            predecessor_item.instrument_id != current_item.instrument_id
            or predecessor_item.fiscal_year != current_item.fiscal_year
            or predecessor_item.report_period != current_item.report_period
        ):
            return (), "legacy_supersedes_chain_scope_mismatch"
        current_id = predecessor_id

    return tuple(reversed(newest_first)), None


def _classification_for_item(
    item: ArchiveInventoryItem,
) -> AnnualReportClassification:
    metadata = _manifest_classification(item.manifest)
    family = item.document_family
    variant = item.variant
    is_full_report = item.is_full_report
    # A correction notice can identify the legal evidence but cannot be
    # promoted to an effective attachment without a complete replacement PDF.
    notice_only = str(item.report_type or "").strip().lower() in {
        "correction_notice",
        "annual_report_notice",
    } or bool(metadata.get("notice_only"))
    if notice_only:
        is_full_report = False
        if variant is None:
            variant = AnnualReportVariant.CORRECTION
    is_eligible = (
        family == DocumentFamily.ANNUAL_REPORT.value
        and variant is not None
        and is_full_report
    )
    return AnnualReportClassification(
        document_family=family,
        fiscal_year=item.fiscal_year,
        report_period=item.report_period,
        variant=variant,
        is_full_report=is_full_report,
        is_eligible=is_eligible,
        correction_evidence=(variant is AnnualReportVariant.CORRECTION),
        reasons=(
            "legacy_correction_notice_evidence"
            if notice_only
            else "legacy_verified_complete_correction"
            if variant is AnnualReportVariant.CORRECTION
            else "legacy_verified_complete_original",
        ),
        vocabulary_version=CLASSIFICATION_VOCABULARY_VERSION,
    )


def _classification_payload(
    classification: AnnualReportClassification,
) -> dict[str, Any]:
    return {
        "document_family": classification.document_family,
        "fiscal_year": classification.fiscal_year,
        "report_period": classification.report_period,
        "variant": None
        if classification.variant is None
        else classification.variant.value,
        "is_full_report": classification.is_full_report,
        "is_eligible": classification.is_eligible,
        "correction_evidence": classification.correction_evidence,
        "reasons": list(classification.reasons),
        "policy_version": classification.policy_version,
        "vocabulary_version": classification.vocabulary_version,
    }


def _legacy_record_and_attachment(
    item: ArchiveInventoryItem,
    *,
    group: Sequence[ArchiveInventoryItem],
    timestamp: str,
) -> tuple[AnnouncementRecord, AnnouncementAttachment, AnnualReportClassification]:
    source = str(item.source)
    filing_id = str(item.filing_id)
    classification = _classification_for_item(item)
    variant_label = (
        "（修订版）"
        if classification.variant is AnnualReportVariant.CORRECTION
        else ""
    )
    metadata = item.manifest.get("metadata") or {}
    title = _text(item.manifest.get("title")) or _text(metadata.get("title"))
    title = title or f"{item.fiscal_year}年年度报告{variant_label}"
    source_url = _text(item.manifest.get("source_url")) or (
        f"https://legacy.invalid/{stable_id('filing', source, filing_id)}.pdf"
    )
    source_file_ids = sorted(
        {member.source_file_id for member in group if member.source_file_id}
    )
    orphan_evidence = _manifest_orphan_evidence(item.manifest)
    attachment_metadata: dict[str, Any] = {
        "content_length": item.content_length,
        "asset_classification": _classification_payload(classification),
        "migration_origin": "legacy_shadow_adoption",
        "legacy_source_file_ids": source_file_ids,
    }
    if orphan_evidence is not None:
        attachment_metadata["orphan_reconciliation"] = dict(orphan_evidence)
    attachment = AnnouncementAttachment(
        source_url=source_url,
        resolved_url=source_url,
        attachment_id=_text(item.manifest.get("attachment_id")) or filing_id,
        name=Path(item.path).name,
        media_type="application/pdf",
        file_extension="pdf",
        raw_metadata=attachment_metadata,
    )
    raw_payload: dict[str, Any] = {
        "migration_origin": "legacy_shadow_adoption",
        "source_file_ids": source_file_ids,
        "observed_at": timestamp,
    }
    if orphan_evidence is not None:
        raw_payload["orphan_reconciliation"] = dict(orphan_evidence)
    record = AnnouncementRecord(
        source=source,
        source_announcement_id=filing_id,
        announcement_key=build_announcement_key(source, filing_id),
        title=title,
        published_at=_published_at(item),
        published_at_raw=item.manifest.get("published_at"),
        exchange=item.exchange,
        market=item.exchange,
        symbols=(str(item.instrument_id).split(".", 1)[0],),
        attachments=(attachment,),
        raw_payload=raw_payload,
        diagnostics=("legacy_shadow_adoption",),
    )
    return record, attachment, classification


def _legacy_legal_chain_id(item: ArchiveInventoryItem) -> str | None:
    if not item.manifest.get("supersedes_source_file_id"):
        return None
    return stable_id(
        "legacy_chain", item.source, item.instrument_id, item.fiscal_year
    )


def _legacy_precedence(
    item: ArchiveInventoryItem,
    all_items: Sequence[ArchiveInventoryItem],
) -> int | None:
    predecessor = _text(item.manifest.get("supersedes_source_file_id"))
    if not predecessor:
        return None
    by_id = {
        candidate.source_file_id: candidate
        for candidate in all_items
        if candidate.source_file_id
    }
    depth = 1
    seen = {item.source_file_id}
    while predecessor and predecessor not in seen:
        seen.add(predecessor)
        previous = by_id.get(predecessor)
        if previous is None:
            break
        depth += 1
        predecessor = _text(previous.manifest.get("supersedes_source_file_id"))
    return depth


def _invalid_correction_candidates(
    inventory: ArchiveInventoryReport,
) -> dict[tuple[str, int], tuple[AnnualReportCandidate, ...]]:
    output: dict[tuple[str, int], list[AnnualReportCandidate]] = defaultdict(list)
    for item in inventory.items:
        if (
            item.status not in {"corrupt", "missing"}
            or not item.instrument_id
            or item.fiscal_year is None
            or item.report_type != "annual_report_correction"
            or not item.source
            or not item.filing_id
        ):
            continue
        classification = _classification_for_item(item)
        output[(item.instrument_id, item.fiscal_year)].append(
            AnnualReportCandidate(
                candidate_id=item.source_file_id
                or stable_id("legacy_invalid", item.source, item.filing_id),
                source=item.source,
                source_announcement_id=item.filing_id,
                attachment_id=item.source_file_id
                or stable_id("legacy_attachment", item.source, item.filing_id),
                content_hash=item.content_hash or item.expected_hash,
                published_at=_published_at(item),
                classification=classification,
                integrity_valid=False,
                legal_chain_id=_legacy_legal_chain_id(item),
            )
        )
    return {scope: tuple(items) for scope, items in output.items()}


def _predecessor_asset_id(
    rows: Sequence[tuple[AnnualReportCandidate, ArchiveInventoryItem, str]],
    *,
    winner_item: ArchiveInventoryItem,
    instrument_id: str,
    fiscal_year: int,
) -> str | None:
    predecessor_source_id = _text(
        winner_item.manifest.get("supersedes_source_file_id")
    )
    if not predecessor_source_id:
        return None
    for candidate, item, _ in rows:
        if item.source_file_id == predecessor_source_id:
            return stable_id(
                "asset", instrument_id, fiscal_year, candidate.candidate_id
            )
    return None


def _catalog_row_matches_asset(
    row: Mapping[str, Any], asset: EffectiveAnnualReport
) -> bool:
    source = _text(row.get("source"))
    filing_id = _text(row.get("filing_id") or row.get("source_announcement_id"))
    content_hash = _text(row.get("content_hash"))
    return bool(
        source
        and filing_id
        and content_hash
        and normalize_source(source) == asset.source
        and filing_id == asset.source_announcement_id
        and content_hash == asset.content_hash
    )


def _verify_legacy_pdf(path: Path, expected_hash: str) -> None:
    if not path.is_file():
        raise FileNotFoundError(path)
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        if handle.read(5) != b"%PDF-":
            raise ValueError(f"legacy file is not a PDF: {path}")
        handle.seek(0)
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != str(expected_hash).lower():
        raise ValueError(f"legacy file hash mismatch: {path}")


def _valid_controlled_legacy_custody_evidence(
    evidence: Mapping[str, Any] | None,
    *,
    path: Path,
    content_hash: str,
    mount_filesystem_key: str,
    config_fingerprint: str,
) -> bool:
    """Accept a legacy path only with exact writer/cleaner shutdown evidence."""

    if not evidence:
        return False
    expected = {
        "path": str(path.resolve(strict=False)),
        "content_hash": str(content_hash).lower(),
        "mount_filesystem_key": str(mount_filesystem_key),
        "config_fingerprint": str(config_fingerprint),
    }
    if any(str(evidence.get(key) or "") != value for key, value in expected.items()):
        return False
    custody_mode = str(evidence.get("custody_mode") or "legacy_processes_disabled")
    if custody_mode == "legacy_processes_disabled":
        if evidence.get("legacy_writer_disabled") is not True:
            return False
        if evidence.get("legacy_cleaner_disabled") is not True:
            return False
    elif custody_mode == "exact_path_excluded":
        # Asset promotion precedes full consumer cutover. During that interval
        # legacy processes may remain active for other files, but both must be
        # proven to exclude this exact hash-qualified path from mutation.
        if evidence.get("legacy_writer_excludes_exact_path") is not True:
            return False
        if evidence.get("legacy_cleaner_excludes_exact_path") is not True:
            return False
    else:
        return False
    if not str(evidence.get("evidence_ref") or "").strip():
        return False
    try:
        datetime.fromisoformat(
            str(evidence.get("verified_at") or "").replace("Z", "+00:00")
        )
    except ValueError:
        return False
    return True


def _legacy_reconciliation_plan_projection(
    row: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    if row is None:
        return None
    return {
        "legacy_path": str(row.get("legacy_path") or ""),
        "consumer": str(row.get("consumer") or ""),
        "asset_id": str(row.get("asset_id") or ""),
        "content_hash": str(row.get("content_hash") or ""),
        "status": str(row.get("status") or ""),
        "manifest_version": str(row.get("manifest_version") or ""),
        "metadata": dict(row.get("metadata") or {}),
    }


def _legacy_reconciliation_evidence_matches(
    row: Mapping[str, Any] | None,
    *,
    legacy_path: str,
    asset_id: str,
    content_hash: str,
    inventory_fingerprint: str,
    config_fingerprint: str,
) -> bool:
    if row is None:
        return False
    metadata = row.get("metadata") or {}
    return bool(
        str(row.get("legacy_path") or "") == str(legacy_path)
        and str(row.get("asset_id") or "") == str(asset_id)
        and str(row.get("content_hash") or "").lower() == str(content_hash).lower()
        and str(row.get("status") or "") == "reconciled_pending_custody"
        and str(metadata.get("inventory_fingerprint") or "")
        == str(inventory_fingerprint)
        and str(metadata.get("config_fingerprint") or "")
        == str(config_fingerprint)
    )


def _backup_state_plan_projection(
    row: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    if row is None:
        return None
    return {
        "config_fingerprint": str(row.get("config_fingerprint") or ""),
        "destination_identity": str(row.get("destination_identity") or ""),
        "failure_domain": str(row.get("failure_domain") or ""),
        "backup_path": str(row.get("backup_path") or ""),
        "content_length": int(row.get("content_length") or 0),
        "status": str(row.get("status") or ""),
        "file_manifest_watermark": str(row.get("file_manifest_watermark") or ""),
        "catalog_snapshot_watermark": str(
            row.get("catalog_snapshot_watermark") or ""
        ),
        "verified_at": str(row.get("verified_at") or ""),
    }


def _capability_plan_projection(
    capability: NfsCapabilityProbe | None,
) -> Mapping[str, Any] | None:
    if capability is None:
        return None
    return {
        "source_path": capability.source_path,
        "target_root": capability.target_root,
        "source_filesystem_key": capability.source_filesystem_key,
        "target_filesystem_key": capability.target_filesystem_key,
        "same_filesystem": capability.same_filesystem,
        "can_hardlink": capability.can_hardlink,
        "can_atomic_rename": capability.can_atomic_rename,
        "probed": capability.probed,
        "reason": capability.reason,
    }


def _require_future_timestamp(value: str | None, *, field_name: str) -> str:
    if not str(value or "").strip():
        raise RuntimeError(f"convergence requires {field_name}")
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    if parsed <= datetime.now(timezone.utc):
        raise RuntimeError(f"{field_name} must be in the future")
    return parsed.astimezone(timezone.utc).isoformat()


def _validate_convergence_backup_state(
    row: Mapping[str, Any] | None,
    *,
    config: AnnouncementAssetConfig,
    content_hash: str,
    content_length: int,
    primary_failure_domain: str,
) -> None:
    if row is None or str(row.get("status") or "") != "verified":
        raise RuntimeError("independent verified backup state is missing")
    if str(row.get("config_fingerprint") or "") != config.config_fingerprint:
        raise RuntimeError("backup configuration fingerprint changed")
    failure_domain = str(row.get("failure_domain") or "")
    if not failure_domain or failure_domain == str(primary_failure_domain):
        raise RuntimeError("backup failure domain is not independently verified")
    expected_failure_domain = str(config.backup.expected_failure_domain or "")
    if expected_failure_domain and failure_domain != expected_failure_domain:
        raise RuntimeError("backup failure domain does not match configuration")
    if int(row.get("content_length") or 0) != int(content_length):
        raise RuntimeError("backup state length does not match blob")
    if not str(row.get("file_manifest_watermark") or "") or not str(
        row.get("catalog_snapshot_watermark") or ""
    ):
        raise RuntimeError("backup state lacks paired watermarks")
    verified_at = str(row.get("verified_at") or "")
    try:
        verified = datetime.fromisoformat(verified_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RuntimeError("backup state verification timestamp is invalid") from exc
    if verified.tzinfo is None:
        verified = verified.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - verified.astimezone(timezone.utc)).total_seconds()
    if age < 0 or age > config.backup.freshness_hours * 3600:
        raise RuntimeError("backup state is stale")
    backup_path = Path(str(row.get("backup_path") or "")).resolve(strict=False)
    destination_root = config.backup.destination_root
    if destination_root is None or not _path_is_beneath_any(
        backup_path, (destination_root.resolve(strict=False),)
    ):
        raise RuntimeError("backup path escapes configured destination")
    if str(row.get("destination_identity") or "") == "":
        raise RuntimeError("backup destination identity is missing")
    if str(content_hash).lower() != str(content_hash):
        raise RuntimeError("backup content hash is not canonical")


def _revalidate_convergence_mounts(
    *,
    legacy: Path,
    root: Path,
    backup_object: Path,
    capability: NfsCapabilityProbe,
    backup_mount_key: str,
    config: AnnouncementAssetConfig,
) -> None:
    ContentAddressedBlobStore(config).validate_mount()
    current = probe_nfs_capabilities(legacy, root, perform_probe=False)
    if (
        current.source_filesystem_key != capability.source_filesystem_key
        or current.target_filesystem_key != capability.target_filesystem_key
    ):
        raise ConvergenceMountRaceError("convergence filesystem identity changed")
    if backup_mount_key != probe_mount_identity(backup_object).filesystem_key:
        raise ConvergenceMountRaceError("backup filesystem identity changed")
    if not os.access(legacy, os.R_OK) or not os.access(root, os.R_OK | os.W_OK):
        raise ConvergenceMountRaceError(
            "convergence mount is not readable and writable"
        )


def _require_safe_canonical_target(root: Path, target: Path) -> None:
    try:
        relative = target.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError as exc:
        raise RuntimeError("canonical target escapes configured root") from exc
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise RuntimeError("canonical target path contains a symlink")
    if target.exists() and target.is_symlink():
        raise RuntimeError("canonical target must not be a symlink")


def _remove_empty_convergence_parents(path: Path, *, stop: Path) -> None:
    """Remove only empty directories created below the configured blob root."""

    boundary = stop.resolve(strict=False)
    current = path.resolve(strict=False)
    while current != boundary:
        try:
            current.relative_to(boundary)
            current.rmdir()
        except (OSError, ValueError):
            return
        current = current.parent


def _verified_copy(
    source: Path,
    target: Path,
    expected_hash: str,
    *,
    before_publish: Callable[[], Any] | None = None,
) -> None:
    temporary = target.parent / f".{target.name}.{os.getpid()}.part"
    try:
        with source.open("rb") as source_handle, temporary.open("xb") as target_handle:
            shutil.copyfileobj(source_handle, target_handle, length=1024 * 1024)
            target_handle.flush()
            os.fsync(target_handle.fileno())
        _verify_legacy_pdf(temporary, expected_hash)
        if before_publish is not None:
            before_publish()
        os.replace(temporary, target)
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
