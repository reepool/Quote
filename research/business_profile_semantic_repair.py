"""Bounded local audit and repair for corrected business-profile semantics.

This service deliberately has no provider, announcement, PDF, or LLM dependency.
It reports derived-state problems from persisted local records and applies only
repairs that can be reconstructed without weakening evidence or review history.
"""

from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

from research.business_profile_checkpoint_lifecycle import (
    delete_owned_checkpoint_file,
    list_owned_checkpoint_files,
    owned_checkpoint_path,
)
from research.business_profile_governance import BusinessProfileRepository
from research.business_profile_activity_production import (
    STORAGE_SEMANTICS_EXTERNAL,
    storage_semantics,
)
from research.business_profile_review import BusinessProfileReviewService
from research.providers.base import ShareholderSnapshot
from research.shareholder_snapshot_policy import (
    actual_shareholder_coverage_scope,
    normalize_shareholder_report_date,
)
from research.shareholder_sync import ShareholderShadowSyncService
from research.business_profile_semantic_runtime import RUNTIME_SCHEMA_VERSION
from utils.date_utils import get_shanghai_time


REPAIR_SCHEMA_VERSION = "business_profile_semantic_repair.v1"
OBSOLETE_WORK_STATUSES = frozenset(
    {"machine_rework", "superseded", "terminal_failure"}
)
MANIFEST_ID_SAMPLE_LIMIT = 100


class BusinessProfileSemanticRepairService:
    """Audit and apply locally reconstructable semantic corrections only."""

    def __init__(
        self,
        storage: Any,
        *,
        checkpoint_root: str | Path | None = None,
    ) -> None:
        self.storage = storage
        self.repository = BusinessProfileRepository(storage)
        self.review_service = BusinessProfileReviewService(self.repository)
        self.checkpoint_root = (
            Path(checkpoint_root) if checkpoint_root is not None else None
        )

    def run(
        self,
        *,
        instrument_ids: Iterable[str] | None = None,
        apply: bool = False,
        all_scope: bool = False,
        result_policy: str = "reuse",
        cleanup_only: bool = False,
        retirement_work_ids: Iterable[str] | None = None,
        retirement_reason: str | None = None,
    ) -> dict[str, Any]:
        result_policy = str(result_policy or "reuse").strip().lower()
        if result_policy not in {"reuse", "replace"}:
            raise ValueError("business-profile repair result_policy must be reuse or replace")
        ids = sorted({str(item).strip() for item in instrument_ids or () if str(item).strip()})
        retired_work_ids = tuple(
            sorted(
                {
                    str(item).strip()
                    for item in retirement_work_ids or ()
                    if str(item).strip()
                }
            )
        )
        marker_reason = str(retirement_reason or "").strip()
        if apply and not ids and not all_scope and not retired_work_ids:
            raise ValueError("repair apply requires instrument_ids or all_scope=True")
        if all_scope and ids:
            raise ValueError("repair accepts instrument_ids or all_scope, not both")
        if retired_work_ids and not marker_reason:
            raise ValueError(
                "retirement_work_ids require a non-empty retirement_reason"
            )
        if retired_work_ids and not apply:
            raise ValueError("retirement marker migration requires apply=True")
        if all_scope:
            ids = (
                self._all_lifecycle_instrument_ids()
                if cleanup_only
                else self._all_local_instrument_ids()
            )

        findings = [
            (
                self._audit_cleanup_instrument(instrument_id)
                if cleanup_only
                else self._audit_instrument(instrument_id)
            )
            for instrument_id in ids
        ]
        execution_state = self._audit_execution_state(
            instrument_ids=None if all_scope else ids,
            include_orphans=all_scope,
        )
        before_projections = self._current_projections(ids)
        result = self._report(
            findings,
            apply=apply,
            result_policy=result_policy,
            before_projections=before_projections,
            execution_state=execution_state,
            cleanup_only=cleanup_only,
        )
        if not apply:
            return result

        changes: list[dict[str, Any]] = []
        for finding in findings:
            instrument_id = finding["instrument_id"]
            try:
                instrument_changes = self._apply_instrument(instrument_id, finding)
                if instrument_changes:
                    changes.extend(instrument_changes)
                else:
                    changes.append({
                        "instrument_id": instrument_id,
                        "status": "unchanged",
                        "reason": "no_local_repair_required",
                    })
            except Exception as exc:  # keep each instrument isolated
                changes.append(
                    {
                        "instrument_id": instrument_id,
                        "status": "failed",
                        "reason": f"repair_exception:{type(exc).__name__}:{exc}",
                    }
                )
        try:
            execution_change = self._cleanup_execution_state(execution_state)
            if execution_change is not None:
                changes.append(execution_change)
        except Exception as exc:
            changes.append(
                {
                    "instrument_id": None,
                    "status": "failed",
                    "reason": (
                        "execution_state_cleanup_exception:"
                        f"{type(exc).__name__}:{exc}"
                    ),
                }
            )
        if retired_work_ids:
            try:
                marker_change = self._write_retirement_markers(
                    retired_work_ids,
                    reason=marker_reason,
                    instrument_ids=ids if not all_scope else None,
                )
                if marker_change is not None:
                    changes.append(marker_change)
            except Exception as exc:
                changes.append(
                    {
                        "instrument_id": None,
                        "status": "failed",
                        "reason": (
                            "retirement_marker_migration_exception:"
                            f"{type(exc).__name__}:{exc}"
                        ),
                    }
                )
        result["changes"] = changes
        result["after_current_projections"] = self._current_projections(ids)
        result["change_counts"] = self._change_counts(changes)
        result["write_count"] = sum(item["status"] == "changed" for item in changes)
        result["deletion_manifest"] = [
            {
                "instrument_id": item.get("instrument_id"),
                "reason": item.get("reason"),
                "affected_ids": list(item.get("affected_ids") or []),
                "affected_count": int(
                    item.get("affected_ids_count")
                    or len(item.get("affected_ids") or [])
                ),
                "affected_ids_hash": item.get("affected_ids_hash"),
                "checkpoint_paths": list(item.get("checkpoint_paths") or []),
                "checkpoint_count": int(
                    item.get("checkpoint_paths_count")
                    or len(item.get("checkpoint_paths") or [])
                ),
                "checkpoint_paths_hash": item.get("checkpoint_paths_hash"),
                "checkpoint_bytes": int(item.get("checkpoint_bytes") or 0),
            }
            for item in changes
            if item.get("status") == "changed"
            and (
                "deleted" in str(item.get("reason") or "")
                or "cleanup" in str(item.get("reason") or "")
            )
        ]
        return result

    def _write_retirement_markers(
        self,
        work_ids: Iterable[str],
        *,
        reason: str,
        instrument_ids: Iterable[str] | None,
    ) -> dict[str, Any] | None:
        """Persist an explicit, immutable retirement decision for selected work.

        Historical rows without a retained rollout phase cannot be classified from
        their field families.  A caller must therefore nominate exact work IDs and
        a reason.  Markers are written in a separate repair invocation from the
        eventual cleanup so the decision remains auditable before deletion.
        """
        requested = tuple(sorted({str(value).strip() for value in work_ids if str(value).strip()}))
        selected_instruments = {
            str(value).strip() for value in instrument_ids or () if str(value).strip()
        }
        now = get_shanghai_time().isoformat()
        changed: list[str] = []
        unchanged: list[str] = []
        held: list[str] = []
        missing: list[str] = []
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            try:
                for work_id in requested:
                    row = conn.execute(
                        "SELECT work_id, instrument_id, status, lease_expires_at, metadata_json "
                        "FROM business_profile_work_items WHERE work_id = ?",
                        (work_id,),
                    ).fetchone()
                    if row is None:
                        missing.append(work_id)
                        continue
                    if selected_instruments and str(row["instrument_id"]) not in selected_instruments:
                        held.append(work_id)
                        continue
                    if (
                        str(row["status"] or "") == "running"
                        and str(row["lease_expires_at"] or "") > now
                    ):
                        held.append(work_id)
                        continue
                    try:
                        metadata = json.loads(row["metadata_json"] or "{}")
                    except (TypeError, ValueError, json.JSONDecodeError) as exc:
                        raise ValueError(
                            f"retirement marker requires object metadata for {work_id}"
                        ) from exc
                    if not isinstance(metadata, dict):
                        raise ValueError(
                            f"retirement marker requires object metadata for {work_id}"
                        )
                    marker = metadata.get("retirement_marker")
                    if marker is not None:
                        if not isinstance(marker, dict):
                            raise ValueError(
                                f"retirement marker is malformed for {work_id}"
                            )
                        if (
                            str(marker.get("reason") or "").strip() != reason
                            or not str(marker.get("timestamp") or "").strip()
                        ):
                            raise ValueError(
                                f"retirement marker is immutable for {work_id}"
                            )
                        unchanged.append(work_id)
                        continue
                    metadata["retirement_marker"] = {
                        "reason": reason,
                        "timestamp": now,
                    }
                    conn.execute(
                        "UPDATE business_profile_work_items "
                        "SET metadata_json = ?, updated_at = ? WHERE work_id = ?",
                        (json.dumps(metadata, ensure_ascii=True, sort_keys=True), now, work_id),
                    )
                    changed.append(work_id)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        if not changed and not unchanged and not held and not missing:
            return None
        return {
            "instrument_id": None,
            "status": "failed" if missing else ("held" if held and not changed else "changed"),
            "reason": "retirement_marker_migrated",
            **self._id_summary(changed, key="affected_ids"),
            "unchanged_ids": unchanged,
            "held_ids": held,
            "missing_ids": missing,
        }

    def _all_lifecycle_instrument_ids(self) -> list[str]:
        """Return instruments that own semantic receipts or run manifests."""

        identifiers: set[str] = set()
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            for table in (
                "business_profile_semantic_artifacts",
                "business_profile_semantic_runs",
            ):
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                    (table,),
                ).fetchone()
                if not exists:
                    continue
                identifiers.update(
                    str(row["instrument_id"] or "").strip()
                    for row in conn.execute(
                        f"SELECT DISTINCT instrument_id FROM {table}"
                    ).fetchall()
                )
        return sorted(item for item in identifiers if item)

    def _all_local_instrument_ids(self) -> list[str]:
        tables = (
            "shareholder_snapshots",
            "company_supply_chain_relationships",
            "company_commodity_exposures",
            "company_business_activities",
            "company_operating_facts",
            "company_value_chain_roles",
            # Semantic lifecycle rows still need instrument-level candidate
            # cleanup. Work/checkpoint state is audited in one bounded pass so
            # retired whole-market shadow queues do not expand this loop.
            "business_profile_semantic_artifacts",
            "business_profile_semantic_runs",
        )
        identifiers: set[str] = set()
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            for table in tables:
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                    (table,),
                ).fetchone()
                if exists:
                    identifiers.update(
                        str(row["instrument_id"] or "").strip()
                        for row in conn.execute(
                            f"SELECT DISTINCT instrument_id FROM {table}"
                        ).fetchall()
                    )
        return sorted(item for item in identifiers if item)

    def _audit_execution_state(
        self,
        *,
        instrument_ids: Iterable[str] | None,
        include_orphans: bool,
    ) -> dict[str, Any]:
        selected_ids = tuple(
            sorted({str(value).strip() for value in instrument_ids or () if str(value).strip()})
        )
        if instrument_ids is not None:
            if not selected_ids:
                rows: list[dict[str, Any]] = []
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            all_rows = [
                dict(row)
                for row in conn.execute(
                    "SELECT work_id, instrument_id, status, stage, lease_expires_at, "
                    "checkpoint_path, metadata_json FROM business_profile_work_items"
                ).fetchall()
            ]
            if instrument_ids is None:
                rows = all_rows
            elif selected_ids:
                selected = set(selected_ids)
                rows = [row for row in all_rows if str(row["instrument_id"]) in selected]

        now = get_shanghai_time().isoformat()
        deletable: list[dict[str, Any]] = []
        retained: list[dict[str, Any]] = []
        active_legacy: list[dict[str, Any]] = []
        reasons: Counter[str] = Counter()
        for row in rows:
            reason = self._obsolete_work_reason(row, now=now)
            if reason is None:
                retained.append(row)
                if self._legacy_shadow_work(row):
                    active_legacy.append(row)
                continue
            item = {**row, "cleanup_reason": reason}
            deletable.append(item)
            reasons[reason] += 1

        checkpoint_files: dict[str, Any] = {
            "enabled": self.checkpoint_root is not None,
            "root": str(self.checkpoint_root) if self.checkpoint_root is not None else None,
            "delete_paths": [],
            "orphan_paths": [],
            "unsafe_paths": [],
            "delete_bytes": 0,
        }
        if self.checkpoint_root is not None:
            deletable_ids = {str(row["work_id"]) for row in deletable}
            retained_paths = {
                candidate
                for row in all_rows
                if str(row["work_id"]) not in deletable_ids
                for candidate in (
                    owned_checkpoint_path(
                        str(row.get("checkpoint_path") or ""),
                        checkpoint_root=self.checkpoint_root,
                    ),
                )
                if candidate is not None
            }
            all_referenced_paths = {
                candidate
                for row in all_rows
                for candidate in (
                    owned_checkpoint_path(
                        str(row.get("checkpoint_path") or ""),
                        checkpoint_root=self.checkpoint_root,
                    ),
                )
                if candidate is not None
            }
            delete_paths: set[Path] = set()
            unsafe_paths: set[str] = set()
            for row in deletable:
                raw_path = str(row.get("checkpoint_path") or "")
                candidate = owned_checkpoint_path(
                    raw_path, checkpoint_root=self.checkpoint_root
                )
                if candidate is None:
                    unsafe_paths.add(raw_path)
                elif candidate not in retained_paths and candidate.is_file():
                    delete_paths.add(candidate)
            orphan_paths: set[Path] = set()
            if include_orphans:
                orphan_paths = {
                    path
                    for path in list_owned_checkpoint_files(self.checkpoint_root)
                    if path not in all_referenced_paths
                }
                delete_paths.update(orphan_paths)
            delete_bytes = 0
            for path in delete_paths:
                try:
                    delete_bytes += int(path.stat().st_size)
                except OSError:
                    continue
            checkpoint_files.update(
                {
                    "delete_paths": [str(path) for path in sorted(delete_paths, key=str)],
                    "orphan_paths": [str(path) for path in sorted(orphan_paths, key=str)],
                    "unsafe_paths": sorted(unsafe_paths),
                    "delete_bytes": delete_bytes,
                }
            )

        issues: list[dict[str, Any]] = []
        if deletable:
            issues.append(
                {
                    "code": "obsolete_work_items",
                    "stable_id": "obsolete_work_items:execution_state",
                    "details": {
                        "count": len(deletable),
                        "reason_counts": dict(reasons),
                        **self._id_summary(
                            [str(row["work_id"]) for row in deletable],
                            key="work_ids",
                        ),
                        **self._id_summary(
                            [str(row["instrument_id"]) for row in deletable],
                            key="instrument_ids",
                        ),
                    },
                }
            )
        if checkpoint_files["orphan_paths"]:
            orphan_summary = self._id_summary(
                checkpoint_files["orphan_paths"], key="paths"
            )
            issues.append(
                {
                    "code": "orphan_checkpoint_files",
                    "stable_id": "orphan_checkpoint_files:execution_state",
                    "details": {
                        "count": len(checkpoint_files["orphan_paths"]),
                        **orphan_summary,
                        "bytes": checkpoint_files["delete_bytes"],
                    },
                }
            )
        if checkpoint_files["unsafe_paths"]:
            issues.append(
                {
                    "code": "unsafe_checkpoint_paths",
                    "stable_id": "unsafe_checkpoint_paths:execution_state",
                    "details": self._id_summary(
                        checkpoint_files["unsafe_paths"], key="paths"
                    ),
                }
            )
        if active_legacy:
            issues.append(
                {
                    "code": "active_legacy_work_items",
                    "stable_id": "active_legacy_work_items:execution_state",
                    "details": {
                        **self._id_summary(
                            [str(row["work_id"]) for row in active_legacy],
                            key="work_ids",
                        ),
                        **self._id_summary(
                            [str(row["instrument_id"]) for row in active_legacy],
                            key="instrument_ids",
                        ),
                    },
                }
            )
        return {
            "include_orphans": include_orphans,
            "work_items": {
                "delete_count": len(deletable),
                "reason_counts": dict(reasons),
                **self._id_summary(
                    [str(row["work_id"]) for row in deletable],
                    key="work_ids",
                ),
                "_delete_work_ids": [str(row["work_id"]) for row in deletable],
            },
            "checkpoint_files": checkpoint_files,
            "issues": issues,
        }

    def _cleanup_execution_state(
        self, audit: dict[str, Any]
    ) -> dict[str, Any] | None:
        requested_ids = tuple(
            str(value)
            for value in (
                audit.get("work_items", {}).get("_delete_work_ids")
                or audit.get("work_items", {}).get("work_ids")
                or ()
            )
        )
        include_orphans = bool(audit.get("include_orphans"))
        deleted_ids: list[str] = []
        selected_paths: list[str] = []
        now = get_shanghai_time().isoformat()
        if requested_ids:
            with self.storage.get_connection() as conn:
                self.storage._apply_pragmas(conn)
                conn.execute("BEGIN IMMEDIATE")
                try:
                    for offset in range(0, len(requested_ids), 800):
                        chunk = requested_ids[offset : offset + 800]
                        placeholders = ",".join("?" for _ in chunk)
                        rows = conn.execute(
                            "SELECT work_id, instrument_id, status, stage, "
                            "lease_expires_at, checkpoint_path, metadata_json "
                            "FROM business_profile_work_items "
                            f"WHERE work_id IN ({placeholders})",
                            chunk,
                        ).fetchall()
                        current_ids = [
                            str(row["work_id"])
                            for row in rows
                            if self._obsolete_work_reason(dict(row), now=now) is not None
                        ]
                        selected_paths.extend(
                            str(row["checkpoint_path"] or "")
                            for row in rows
                            if str(row["work_id"]) in current_ids
                        )
                        if current_ids:
                            current_placeholders = ",".join("?" for _ in current_ids)
                            conn.execute(
                                "DELETE FROM business_profile_work_items "
                                f"WHERE work_id IN ({current_placeholders})",
                                tuple(current_ids),
                            )
                            deleted_ids.extend(current_ids)
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

        deleted_paths: list[str] = []
        deleted_bytes = 0
        file_failures: list[dict[str, str]] = []
        if self.checkpoint_root is not None:
            with self.storage.get_connection() as conn:
                self.storage._apply_pragmas(conn)
                retained_paths = {
                    candidate
                    for row in conn.execute(
                        "SELECT checkpoint_path FROM business_profile_work_items"
                    ).fetchall()
                    for candidate in (
                        owned_checkpoint_path(
                            str(row["checkpoint_path"] or ""),
                            checkpoint_root=self.checkpoint_root,
                        ),
                    )
                    if candidate is not None
                }
            candidates: set[Path] = set()
            for raw_path in selected_paths:
                candidate = owned_checkpoint_path(
                    raw_path, checkpoint_root=self.checkpoint_root
                )
                if candidate is None:
                    file_failures.append(
                        {"path": raw_path, "reason": "checkpoint_path_outside_owned_root"}
                    )
                elif candidate not in retained_paths:
                    candidates.add(candidate)
            if include_orphans:
                candidates.update(
                    path
                    for path in list_owned_checkpoint_files(self.checkpoint_root)
                    if path not in retained_paths
                )
            for path in sorted(candidates, key=str):
                try:
                    size = int(path.stat().st_size) if path.is_file() else 0
                    if delete_owned_checkpoint_file(
                        path, checkpoint_root=self.checkpoint_root
                    ):
                        deleted_paths.append(str(path))
                        deleted_bytes += size
                except (OSError, ValueError) as exc:
                    file_failures.append(
                        {"path": str(path), "reason": f"{type(exc).__name__}:{exc}"}
                    )

        if not deleted_ids and not deleted_paths and not file_failures:
            return None
        affected_summary = self._id_summary(deleted_ids, key="affected_ids")
        checkpoint_summary = self._id_summary(
            deleted_paths, key="checkpoint_paths"
        )
        return {
            "instrument_id": None,
            "status": "failed" if file_failures else "changed",
            "reason": (
                "execution_state_cleanup_incomplete"
                if file_failures
                else "obsolete_execution_state_deleted"
            ),
            **affected_summary,
            **checkpoint_summary,
            "checkpoint_bytes": deleted_bytes,
            "file_failures": file_failures,
        }

    @staticmethod
    def _id_summary(values: Iterable[str], *, key: str) -> dict[str, Any]:
        ordered = sorted({str(value) for value in values if str(value)})
        digest = hashlib.sha256("\n".join(ordered).encode("utf-8")).hexdigest()
        return {
            key: ordered[:MANIFEST_ID_SAMPLE_LIMIT],
            f"{key}_count": len(ordered),
            f"{key}_hash": digest,
            f"{key}_truncated": len(ordered) > MANIFEST_ID_SAMPLE_LIMIT,
        }

    @staticmethod
    def _legacy_shadow_work(row: dict[str, Any]) -> bool:
        try:
            metadata = json.loads(row.get("metadata_json") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            metadata = {}
        processing_identity = (
            dict(metadata.get("processing_identity") or {})
            if isinstance(metadata, dict)
            else {}
        )
        if str(processing_identity.get("rollout_phase") or "") == "structured_shadow":
            return True
        marker = metadata.get("retirement_marker") if isinstance(metadata, dict) else None
        return (
            isinstance(marker, dict)
            and bool(str(marker.get("reason") or "").strip())
            and bool(str(marker.get("timestamp") or "").strip())
        )

    @classmethod
    def _obsolete_work_reason(
        cls, row: dict[str, Any], *, now: str
    ) -> str | None:
        status = str(row.get("status") or "")
        if status in OBSOLETE_WORK_STATUSES:
            return f"work_status_{status}"
        if not cls._legacy_shadow_work(row):
            return None
        if status == "running" and str(row.get("lease_expires_at") or "") > now:
            return None
        return "retired_structured_shadow"

    def _audit_instrument(self, instrument_id: str) -> dict[str, Any]:
        issues: list[dict[str, Any]] = []
        inventory = self._inventory_instrument(instrument_id)
        snapshot = self.storage.get_shareholder_snapshot(instrument_id, include_snapshot=True)
        if snapshot is not None:
            payload = snapshot.get("snapshot") if isinstance(snapshot.get("snapshot"), dict) else {}
            claimed = {str(item).strip() for item in payload.get("coverage_scope") or () if str(item).strip()}
            actual = actual_shareholder_coverage_scope(
                exchange=str(snapshot.get("exchange") or ""),
                snapshot_json=payload,
                holder_count=snapshot.get("holder_count"),
            )
            if claimed != actual:
                issues.append(self._issue("shareholder_scope_mismatch", instrument_id, {
                    "claimed": sorted(claimed), "actual": sorted(actual),
                }))
            for scope_date in self._snapshot_dates(payload):
                normalized = normalize_shareholder_report_date(scope_date)
                if normalized and normalized != str(scope_date):
                    issues.append(self._issue("shareholder_noncanonical_report_date", instrument_id, {
                        "value": str(scope_date), "normalized": normalized,
                    }))
            controller_finding = self._controller_provenance_finding(
                snapshot, payload
            )
            if controller_finding is not None:
                issues.append(
                    self._issue(
                        controller_finding["code"],
                        instrument_id,
                        controller_finding["details"],
                    )
                )

        relationships = self.repository.list_records("relationships", instrument_id=instrument_id, limit=10000)
        for record in relationships:
            metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
            if (
                record.get("review_status") == "approved"
                and self._relationship_resolution_basis(record, metadata)
                in {
                    "exact_legal_name",
                    "approved_exact_alias",
                    "approved_exact_legal_name",
                }
                and self._is_short_name_resolution(record)
            ):
                issues.append(self._issue("relationship_short_name_auto_resolution", instrument_id, {
                    "relationship_id": record.get("relationship_id"),
                    "counterparty_name_raw": record.get("counterparty_name_raw"),
                    "resolution_basis": self._relationship_resolution_basis(record, metadata),
                }))
        issues.extend(self._activity_and_role_findings(instrument_id))
        issues.extend(self._operating_fact_findings(instrument_id))
        issues.extend(self._incompatible_artifact_findings(instrument_id))
        issues.extend(self._exposure_collision_findings(instrument_id))
        return {"instrument_id": instrument_id, "inventory": inventory, "issues": issues}

    def _audit_cleanup_instrument(self, instrument_id: str) -> dict[str, Any]:
        """Audit only lifecycle rows that are eligible for physical deletion."""

        return {
            "instrument_id": instrument_id,
            "inventory": self._inventory_instrument(instrument_id),
            "issues": self._incompatible_artifact_findings(instrument_id),
        }

    def _inventory_instrument(self, instrument_id: str) -> dict[str, Any]:
        """Return a read-only lifecycle inventory used by repair and operators."""
        inventory: dict[str, Any] = {
            "semantic_receipts": Counter(),
            "work_items": Counter(),
            "candidate_records": Counter(),
            "approved_records": Counter(),
        }
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            tables = {
                str(row[0]) for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "business_profile_semantic_artifacts" in tables:
                rows = conn.execute(
                    "SELECT e.status, COUNT(*) AS n "
                    "FROM business_profile_semantic_artifacts a "
                    "JOIN business_profile_semantic_artifact_events e "
                    "ON e.artifact_id = a.artifact_id "
                    "WHERE a.instrument_id = ? AND e.rowid IN "
                    "(SELECT MAX(rowid) FROM business_profile_semantic_artifact_events GROUP BY artifact_id) "
                    "GROUP BY e.status",
                    (instrument_id,),
                ).fetchall()
                inventory["semantic_receipts"] = Counter({str(r[0]): int(r[1]) for r in rows})
            if "business_profile_work_items" in tables:
                rows = conn.execute(
                    "SELECT status, COUNT(*) AS n FROM business_profile_work_items "
                    "WHERE instrument_id = ? GROUP BY status",
                    (instrument_id,),
                ).fetchall()
                inventory["work_items"] = Counter({str(r[0]): int(r[1]) for r in rows})
            record_tables = {
                "activities": "company_business_activities",
                "operating_facts": "company_operating_facts",
                "relationships": "company_supply_chain_relationships",
                "value_chain_roles": "company_value_chain_roles",
                "exposures": "company_commodity_exposures",
                "exposure_facts": "company_commodity_exposure_facts",
            }
            for kind, table in record_tables.items():
                if table not in tables:
                    continue
                rows = conn.execute(
                    f"SELECT review_status, COUNT(*) AS n FROM {table} "
                    "WHERE instrument_id = ? GROUP BY review_status",
                    (instrument_id,),
                ).fetchall()
                for row in rows:
                    status = str(row[0] or "")
                    target = inventory["candidate_records"] if status == "candidate" else inventory["approved_records"]
                    target[kind] += int(row[1])
        return {key: dict(value) for key, value in inventory.items()}

    def _incompatible_artifact_findings(self, instrument_id: str) -> list[dict[str, Any]]:
        """Report completed artifacts that cannot safely satisfy current reuse."""

        findings: list[dict[str, Any]] = []
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='business_profile_semantic_runs'"
            ).fetchone()
            if table is None:
                return findings
            rows = conn.execute(
                "SELECT run_id, field_family, source_document_id, status, metadata_json "
                "FROM business_profile_semantic_runs "
                "WHERE instrument_id = ?",
                (instrument_id,),
            ).fetchall()
            artifact_rows = conn.execute(
                "SELECT artifact_id, source_document_id, field_family, response_json "
                "FROM business_profile_semantic_artifacts WHERE instrument_id = ?",
                (instrument_id,),
            ).fetchall()
            event_rows = conn.execute(
                "SELECT artifact_id, status, reason_code FROM business_profile_semantic_artifact_events "
                "WHERE artifact_id IN (SELECT artifact_id FROM business_profile_semantic_artifacts WHERE instrument_id = ?) "
                "AND rowid IN (SELECT MAX(rowid) FROM business_profile_semantic_artifact_events GROUP BY artifact_id)",
                (instrument_id,),
            ).fetchall()
        artifact_ids = {str(row["artifact_id"]) for row in artifact_rows}
        for row in rows:
            try:
                metadata = json.loads(row["metadata_json"] or "{}")
            except (TypeError, ValueError):
                metadata = {}
            artifact_id = str(metadata.get("semantic_artifact_id") or "").strip()
            if str(row["status"] or "") == "superseded":
                findings.append(
                    self._issue(
                        "superseded_semantic_run",
                        instrument_id,
                        {
                            "run_id": row["run_id"],
                            "field_family": row["field_family"],
                            "source_document_id": row["source_document_id"],
                            "proposed_action": "delete_obsolete_run_manifest",
                        },
                    )
                )
                continue
            if self._legacy_shadow_work({"metadata_json": row["metadata_json"]}):
                if not artifact_id or artifact_id not in artifact_ids:
                    findings.append(
                        self._issue(
                            "legacy_shadow_semantic_run",
                            instrument_id,
                            {
                                "run_id": row["run_id"],
                                "field_family": row["field_family"],
                                "source_document_id": row["source_document_id"],
                                "proposed_action": "delete_obsolete_run_manifest",
                            },
                        )
                    )
                continue
            if metadata.get("reuse_blocked") is True:
                continue
            if str(metadata.get("result_policy") or "reuse") != "reuse":
                continue
            if str(metadata.get("runtime_schema_version") or "") == RUNTIME_SCHEMA_VERSION:
                continue
            # A stale runtime version alone does not make a completed result
            # garbage.  Only runs whose persisted record identities are
            # demonstrably legacy are unsafe to reuse.  Receipts with explicit
            # failure/legacy payloads are classified separately below.
            if not self._metadata_has_legacy_occurrence_identity(metadata):
                continue
            findings.append(
                self._issue(
                    "incompatible_reusable_artifact",
                    instrument_id,
                    {
                        "run_id": row["run_id"],
                        "field_family": row["field_family"],
                        "source_document_id": row["source_document_id"],
                        "artifact_id": metadata.get("semantic_artifact_id"),
                        "artifact_runtime_schema_version": metadata.get(
                            "runtime_schema_version"
                        ),
                        "expected_runtime_schema_version": RUNTIME_SCHEMA_VERSION,
                        "proposed_action": "local_replay_or_bounded_reextract",
                    },
                )
            )
        # Also inspect receipts directly. A receipt can predate the current run
        # manifest, and retired shadow receipts must never remain replayable.
        latest_event = {
            str(row["artifact_id"]): {
                "status": str(row["status"] or ""),
                "reason_code": str(row["reason_code"] or ""),
            }
            for row in event_rows
        }
        known = {
            str(item.get("details", {}).get("artifact_id") or "")
            for item in findings
        }
        retired_artifact_ids = {
            str(json.loads(row["metadata_json"] or "{}").get("semantic_artifact_id") or "")
            for row in rows
            if self._legacy_shadow_work({"metadata_json": row["metadata_json"]})
        }
        for row in artifact_rows:
            artifact_id = str(row["artifact_id"])
            try:
                response = json.loads(row["response_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                response = {}
            legacy = self._response_has_legacy_occurrence_identity(response)
            event = latest_event.get(artifact_id, {})
            latest_status = str(event.get("status") or "")
            latest_reason = str(event.get("reason_code") or "")
            failed = latest_status == "rejected" or (
                latest_status == "conversion_pending"
                and not latest_reason.startswith("unit_rule_")
            )
            legacy_shadow = artifact_id in retired_artifact_ids
            if not legacy and not failed and not legacy_shadow:
                continue
            if artifact_id in known:
                continue
            issue_code = (
                "legacy_shadow_semantic_artifact"
                if legacy_shadow
                else "failed_semantic_artifact"
                if failed
                else "legacy_semantic_artifact"
            )
            findings.append(
                self._issue(
                    issue_code,
                    instrument_id,
                    {
                        "artifact_id": artifact_id,
                        "field_family": row["field_family"],
                        "source_document_id": row["source_document_id"],
                        "latest_status": latest_status,
                        "latest_reason_code": latest_reason,
                        "legacy_occurrence_identity": legacy,
                        "proposed_action": "delete_and_reextract",
                    },
                )
            )
        return findings

    def _metadata_has_legacy_occurrence_identity(self, metadata: dict[str, Any]) -> bool:
        """Check persisted record ids before treating an old run as garbage."""

        record_ids = dict(metadata.get("record_ids") or {})
        for record_type, values in record_ids.items():
            for record_id in values or ():
                record = self.repository.get_record(str(record_type), str(record_id))
                if not isinstance(record, dict):
                    continue
                if str(record_type) == "activities":
                    if str(record.get("action") or "") not in {
                        "produces", "sells", "purchases", "consumes", "stores", "transports"
                    }:
                        continue
                    metadata_row = record.get("metadata") or {}
                    if not str(metadata_row.get("source_row_key") or "").strip() and not str(
                        metadata_row.get("contract_reference_raw") or ""
                    ).strip():
                        return True
                elif str(record_type) == "operating_facts":
                    metadata_row = record.get("metadata") or {}
                    if not str(metadata_row.get("source_row_key") or "").strip():
                        return True
        return False

    @staticmethod
    def _response_has_legacy_occurrence_identity(response: Any) -> bool:
        if not isinstance(response, dict):
            return True
        for row in response.get("activities") or ():
            if not isinstance(row, dict):
                continue
            if str(row.get("action") or "") not in {
                "produces", "sells", "purchases", "consumes", "stores", "transports"
            }:
                continue
            if row.get("value") in (None, "") and row.get("unit") in (None, ""):
                continue
            if not str(row.get("source_row_key") or "").strip() and not str(
                row.get("contract_reference_raw") or ""
            ).strip():
                return True
        return any(
            isinstance(row, dict) and not str(row.get("source_row_key") or "").strip()
            for row in response.get("operating_facts") or ()
        )

    def _activity_and_role_findings(self, instrument_id: str) -> list[dict[str, Any]]:
        activities = self.repository.list_records(
            "activities", instrument_id=instrument_id, limit=10000
        )
        by_id = {
            str(item.get("activity_id") or ""): item
            for item in activities
            if str(item.get("activity_id") or "")
        }
        roles = self.repository.list_records(
            "value_chain_roles", instrument_id=instrument_id, limit=10000
        )
        findings: list[dict[str, Any]] = []
        role_groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for role in roles:
            metadata = role.get("metadata") if isinstance(role.get("metadata"), dict) else {}
            supports = []
            for raw_activity_id in metadata.get("supporting_activity_ids") or ():
                activity_id = str(raw_activity_id or "")
                if activity_id in by_id:
                    supports.append(by_id[activity_id])
            if (
                role.get("review_status") in {"approved", "candidate"}
                and role.get("role") == "storage_provider"
                and supports
                and all(
                    str(activity.get("action") or "") == "stores"
                    and storage_semantics(activity) != STORAGE_SEMANTICS_EXTERNAL
                    for activity in supports
                )
            ):
                findings.append(
                    self._issue(
                        "inventory_derived_storage_role",
                        instrument_id,
                        {
                            "record_id": role.get("record_id"),
                            "supporting_activity_ids": [
                                item.get("activity_id") for item in supports
                            ],
                        },
                    )
                )
            if role.get("review_status") not in {"approved", "candidate"}:
                continue
            key = (
                role.get("instrument_id"),
                role.get("segment_id"),
                role.get("role"),
                role.get("report_period"),
                role.get("business_regime_id"),
            )
            role_groups.setdefault(key, []).append(role)
        for key, group in role_groups.items():
            if len(group) > 1:
                findings.append(
                    self._issue(
                        "duplicate_role_business_identity",
                        instrument_id,
                        {
                            "identity": list(key),
                            "record_ids": [item.get("record_id") for item in group],
                        },
                    )
                )
        return findings

    def _operating_fact_findings(self, instrument_id: str) -> list[dict[str, Any]]:
        records = self.repository.list_records(
            "operating_facts", instrument_id=instrument_id, limit=10000
        )
        groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for record in records:
            if record.get("review_status") not in {"approved", "candidate"}:
                continue
            scope = str(record.get("fact_scope") or "").split("#", 1)[0]
            key = (
                record.get("report_period"),
                record.get("segment_id"),
                record.get("fact_type"),
                scope,
                record.get("unit_normalized"),
            )
            groups.setdefault(key, []).append(record)
        findings: list[dict[str, Any]] = []
        for key, group in groups.items():
            identities = {
                str((item.get("metadata") or {}).get("source_row_key") or "")
                for item in group
            }
            values = {
                (item.get("value_raw"), item.get("unit_raw")) for item in group
            }
            if len(group) > 1 and len(values) > 1 and len(identities) <= 1:
                findings.append(
                    self._issue(
                        "operating_fact_occurrence_conflict",
                        instrument_id,
                        {
                            "identity": list(key),
                            "record_ids": [item.get("record_id") for item in group],
                            "values": [list(value) for value in sorted(values, key=str)],
                            "reconstructable": len(
                                {
                                    str(item.get("evidence_id") or "")
                                    + ":"
                                    + str((item.get("metadata") or {}).get("exact_evidence") or "")
                                    for item in group
                                }
                            )
                            == len(group),
                        },
                    )
                )
        return findings

    def _apply_instrument(self, instrument_id: str, finding: dict[str, Any]) -> list[dict[str, Any]]:
        changes: list[dict[str, Any]] = []
        issue_codes = {str(item["code"]) for item in finding["issues"]}
        snapshot = self.storage.get_shareholder_snapshot(instrument_id, include_snapshot=True)
        if snapshot is not None and issue_codes & {
            "shareholder_scope_mismatch", "shareholder_noncanonical_report_date", "shareholder_inferred_controller"
        }:
            rebuilt = self._rebuild_snapshot(snapshot, clear_inferred_controller=("shareholder_inferred_controller" in issue_codes))
            stats = self.storage.upsert_shareholder_snapshot(rebuilt, return_stats=True) or {}
            changes.append({
                "instrument_id": instrument_id,
                "status": "changed" if not stats.get("unchanged") else "unchanged",
                "reason": "shareholder_snapshot_reconstructed_locally",
            })
        for issue in finding["issues"]:
            if issue["code"] == "relationship_short_name_auto_resolution":
                changes.append(
                    self._hold_unsafe_relationship(instrument_id, issue)
                )
            elif issue["code"] in {
                "shareholder_controller_provenance_ambiguous",
                "exposure_action_collision",
            }:
                changes.append({
                    "instrument_id": instrument_id,
                    "status": "held",
                    "reason": issue["code"],
                    "stable_id": issue["stable_id"],
                })
            elif issue["code"] == "inventory_derived_storage_role":
                changes.append(self._hold_invalid_role(instrument_id, issue))
            elif issue["code"] in {
                "duplicate_role_business_identity",
            }:
                changes.append(self._deduplicate_machine_roles(instrument_id, issue))
            elif issue["code"] == "operating_fact_occurrence_conflict":
                if issue.get("details", {}).get("reconstructable"):
                    changes.extend(
                        self._replay_operating_fact_group(
                            instrument_id,
                            issue,
                        )
                    )
                else:
                    changes.append({
                        "instrument_id": instrument_id,
                        "status": "held",
                        "reason": issue["code"],
                        "stable_id": issue["stable_id"],
                    })
            elif issue["code"] in {
                "incompatible_reusable_artifact",
                "legacy_semantic_artifact",
                "legacy_shadow_semantic_artifact",
                "failed_semantic_artifact",
            }:
                changes.append(self._delete_unusable_artifact(instrument_id, issue))
            elif issue["code"] in {
                "legacy_shadow_semantic_run",
                "superseded_semantic_run",
            }:
                changes.append(self._delete_obsolete_semantic_run(instrument_id, issue))
        return changes

    def _deduplicate_machine_roles(
        self, instrument_id: str, issue: dict[str, Any]
    ) -> dict[str, Any]:
        record_ids = [str(value) for value in issue.get("details", {}).get("record_ids") or ()]
        if len(record_ids) < 2:
            return {"instrument_id": instrument_id, "status": "unchanged", "reason": "duplicate_role_already_resolved", "stable_id": issue["stable_id"]}
        records: list[dict[str, Any]] = []
        for record_id in record_ids:
            record = self.repository.get_record("value_chain_roles", record_id)
            if record and record.get("review_status") in {"approved", "candidate"}:
                records.append(record)
        if len(records) < 2:
            return {"instrument_id": instrument_id, "status": "unchanged", "reason": "duplicate_role_already_resolved", "stable_id": issue["stable_id"]}
        if any(
            not str(
                (record.get("metadata") or {}).get("role_rule_version") or ""
            ).startswith("business_profile_")
            for record in records
        ):
            return {"instrument_id": instrument_id, "status": "held", "reason": "duplicate_role_requires_review", "stable_id": issue["stable_id"]}

        # Prefer the oldest approved machine role as the current canonical
        # identity.  The role is a company/report capability; the other rows
        # are duplicate evidence projections, not independent roles.
        records.sort(
            key=lambda record: (
                record.get("review_status") != "approved",
                str(record.get("created_at") or ""),
                str(record.get("record_id") or ""),
            )
        )
        canonical = records[0]
        canonical_id = str(canonical.get("record_id") or "")
        duplicate_records = records[1:]
        for record in duplicate_records:
            if record.get("review_status") != "approved":
                continue
            audits = self.review_service.list_review_audit(
                record_type="value_chain_roles",
                record_id=str(record.get("record_id") or ""),
            )
            if any(
                not str(audit.get("reviewer") or "").startswith(
                    ("system:", "automation:")
                )
                for audit in audits
            ):
                return {"instrument_id": instrument_id, "status": "held", "reason": "duplicate_role_requires_review", "stable_id": issue["stable_id"]}

        removable = [
            str(record.get("record_id") or "")
            for record in duplicate_records
            if record.get("review_status") == "candidate"
        ]
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            try:
                for record_id in removable:
                    conn.execute(
                        "DELETE FROM company_value_chain_roles WHERE record_id = ? "
                        "AND instrument_id = ? AND review_status IN ('approved', 'candidate')",
                        (record_id, instrument_id),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        held_ids: list[str] = []
        audit_ids: list[str] = []
        for record in duplicate_records:
            if record.get("review_status") != "approved":
                continue
            record_id = str(record.get("record_id") or "")
            audit = self.review_service.system_hold_approved_record(
                "value_chain_roles",
                record_id,
                expected_updated_at=str(record.get("updated_at") or ""),
                reason="duplicate machine-derived role consolidated into canonical role",
                metadata={
                    "repair_issue_id": issue["stable_id"],
                    "canonical_record_id": canonical_id,
                },
            )
            held_ids.append(record_id)
            audit_ids.append(str(audit.get("audit_id") or ""))
        return {
            "instrument_id": instrument_id,
            "status": "changed",
            "reason": "duplicate_machine_roles_consolidated",
            "stable_id": issue["stable_id"],
            "canonical_record_id": canonical_id,
            "affected_ids": [*removable, *held_ids],
            "review_audit_ids": audit_ids,
        }

    def _delete_unusable_artifact(
        self, instrument_id: str, issue: dict[str, Any]
    ) -> dict[str, Any]:
        """Physically remove a failed/legacy model receipt and its junk outputs.

        Candidate records explicitly listed by the failed semantic run, plus
        candidate descendants directly linked by ``run_id``, are removed.
        Evidence, approved history, and review audit rows remain immutable.
        Removing the receipt prevents the failed response from being selected
        again; the next backfill performs fresh extraction.
        """
        artifact_id = str(issue.get("details", {}).get("artifact_id") or "").strip()
        run_id = str(issue.get("details", {}).get("run_id") or "").strip()
        if not artifact_id and run_id:
            with self.storage.get_connection() as conn:
                self.storage._apply_pragmas(conn)
                conn.execute("BEGIN IMMEDIATE")
                try:
                    row = conn.execute(
                        "SELECT metadata_json FROM business_profile_semantic_runs "
                        "WHERE run_id = ? AND instrument_id = ?",
                        (run_id, instrument_id),
                    ).fetchone()
                    if row is None:
                        conn.rollback()
                        return {
                            "instrument_id": instrument_id,
                            "status": "unchanged",
                            "reason": "legacy_run_already_absent",
                            "stable_id": issue["stable_id"],
                        }
                    metadata = json.loads(row["metadata_json"] or "{}")
                    deleted_records = self._delete_candidate_records_from_metadata(
                        conn, instrument_id, metadata
                    )
                    deleted_records.extend(
                        self._delete_candidate_records_from_run_id(
                            conn, instrument_id, run_id
                        )
                    )
                    conn.execute(
                        "DELETE FROM business_profile_semantic_runs "
                        "WHERE run_id = ? AND instrument_id = ?",
                        (run_id, instrument_id),
                    )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
            return {
                "instrument_id": instrument_id,
                "status": "changed",
                "reason": "incompatible_legacy_run_deleted",
                "stable_id": issue["stable_id"],
                "affected_ids": [run_id, *deleted_records],
            }
        if not artifact_id:
            return {
                "instrument_id": instrument_id,
                "status": "held",
                "reason": "artifact_identity_unavailable",
                "stable_id": issue["stable_id"],
            }
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT instrument_id FROM business_profile_semantic_artifacts "
                    "WHERE artifact_id = ?",
                    (artifact_id,),
                ).fetchone()
                if row is None or str(row["instrument_id"]) != instrument_id:
                    conn.rollback()
                    return {
                        "instrument_id": instrument_id,
                        "status": "unchanged",
                        "reason": "artifact_already_absent",
                        "stable_id": issue["stable_id"],
                    }
                run_rows = conn.execute(
                    "SELECT run_id, metadata_json FROM business_profile_semantic_runs "
                    "WHERE instrument_id = ? AND metadata_json LIKE ?",
                    (instrument_id, f"%{artifact_id}%"),
                ).fetchall()
                deleted_records: list[str] = []
                for run_row in run_rows:
                    try:
                        run_metadata = json.loads(run_row["metadata_json"] or "{}")
                    except (TypeError, ValueError, json.JSONDecodeError):
                        run_metadata = {}
                    deleted_records.extend(
                        self._delete_candidate_records_from_metadata(
                            conn, instrument_id, run_metadata
                        )
                    )
                    deleted_records.extend(
                        self._delete_candidate_records_from_run_id(
                            conn, instrument_id, str(run_row["run_id"])
                        )
                    )
                conn.execute(
                    "DELETE FROM business_profile_semantic_artifact_events WHERE artifact_id = ?",
                    (artifact_id,),
                )
                # Unit observations are diagnostics derived from the same
                # semantic receipt.  They are not source evidence and must be
                # removed with the receipt to satisfy the foreign-key contract.
                conn.execute(
                    "DELETE FROM business_profile_unit_rule_observations WHERE artifact_id = ?",
                    (artifact_id,),
                )
                conn.execute(
                    "DELETE FROM business_profile_semantic_artifacts WHERE artifact_id = ?",
                    (artifact_id,),
                )
                if run_id:
                    conn.execute(
                        "DELETE FROM business_profile_semantic_runs WHERE run_id = ? "
                        "AND instrument_id = ?",
                        (run_id, instrument_id),
                    )
                # Some completed manifests only retain the receipt id inside
                # metadata.  Remove those obsolete manifests as well; they
                # cannot be selected once their receipt is gone.
                conn.execute(
                    "DELETE FROM business_profile_semantic_runs WHERE instrument_id = ? "
                    "AND metadata_json LIKE ?",
                    (instrument_id, f"%{artifact_id}%"),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return {
            "instrument_id": instrument_id,
            "status": "changed",
            "reason": "unusable_semantic_artifact_deleted",
            "stable_id": issue["stable_id"],
            "affected_ids": [artifact_id, *dict.fromkeys(deleted_records)],
        }

    def _delete_obsolete_semantic_run(
        self, instrument_id: str, issue: dict[str, Any]
    ) -> dict[str, Any]:
        """Delete one obsolete run manifest and only its candidate descendants."""

        run_id = str(issue.get("details", {}).get("run_id") or "").strip()
        if not run_id:
            return {
                "instrument_id": instrument_id,
                "status": "failed",
                "reason": "semantic_run_identity_unavailable",
                "stable_id": issue["stable_id"],
            }
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT metadata_json FROM business_profile_semantic_runs "
                    "WHERE run_id = ? AND instrument_id = ?",
                    (run_id, instrument_id),
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return {
                        "instrument_id": instrument_id,
                        "status": "unchanged",
                        "reason": "semantic_run_already_absent",
                        "stable_id": issue["stable_id"],
                    }
                try:
                    metadata = json.loads(row["metadata_json"] or "{}")
                except (TypeError, ValueError, json.JSONDecodeError):
                    metadata = {}
                deleted_records = self._delete_candidate_records_from_metadata(
                    conn, instrument_id, metadata
                )
                deleted_records.extend(
                    self._delete_candidate_records_from_run_id(
                        conn, instrument_id, run_id
                    )
                )
                conn.execute(
                    "DELETE FROM business_profile_semantic_runs "
                    "WHERE run_id = ? AND instrument_id = ?",
                    (run_id, instrument_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return {
            "instrument_id": instrument_id,
            "status": "changed",
            "reason": "obsolete_semantic_run_deleted",
            "stable_id": issue["stable_id"],
            "affected_ids": [run_id, *dict.fromkeys(deleted_records)],
        }

    @staticmethod
    def _delete_candidate_records_from_run_id(
        conn: Any, instrument_id: str, run_id: str
    ) -> list[str]:
        """Delete candidate descendants directly linked to an obsolete run."""

        tables = (
            ("exposure_facts", "company_commodity_exposure_facts", "fact_id"),
            ("relationships", "company_supply_chain_relationships", "relationship_id"),
            ("activities", "company_business_activities", "activity_id"),
        )
        existing_tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        deleted: list[str] = []
        for record_type, table, primary_key in tables:
            if table not in existing_tables:
                continue
            record_ids = [
                str(row[0])
                for row in conn.execute(
                    f"SELECT {primary_key} FROM {table} "
                    "WHERE instrument_id = ? AND run_id = ? "
                    "AND review_status = 'candidate'",
                    (instrument_id, run_id),
                ).fetchall()
            ]
            for record_id in record_ids:
                if "business_profile_exceptions" in existing_tables:
                    conn.execute(
                        "DELETE FROM business_profile_exceptions WHERE target_id = ? "
                        "AND instrument_id = ?",
                        (record_id, instrument_id),
                    )
                conn.execute(
                    f"DELETE FROM {table} WHERE {primary_key} = ? "
                    "AND instrument_id = ? AND run_id = ? "
                    "AND review_status = 'candidate'",
                    (record_id, instrument_id, run_id),
                )
                deleted.append(f"{record_type}:{record_id}")
        return deleted

    @staticmethod
    def _delete_candidate_records_from_metadata(
        conn: Any, instrument_id: str, metadata: dict[str, Any]
    ) -> list[str]:
        """Delete only candidate outputs explicitly recorded by one semantic run."""

        tables = {
            "activities": ("company_business_activities", "activity_id"),
            "operating_facts": ("company_operating_facts", "record_id"),
            "relationships": ("company_supply_chain_relationships", "relationship_id"),
            "value_chain_roles": ("company_value_chain_roles", "record_id"),
            "exposures": ("company_commodity_exposures", "exposure_id"),
            "exposure_facts": ("company_commodity_exposure_facts", "fact_id"),
        }
        existing_tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        deleted: list[str] = []
        for record_type, record_ids in dict(metadata.get("record_ids") or {}).items():
            table_spec = tables.get(str(record_type))
            if table_spec is None or table_spec[0] not in existing_tables:
                continue
            table, primary_key = table_spec
            for raw_id in record_ids or ():
                record_id = str(raw_id or "").strip()
                if not record_id:
                    continue
                row = conn.execute(
                    f"SELECT review_status FROM {table} WHERE {primary_key} = ? "
                    "AND instrument_id = ?",
                    (record_id, instrument_id),
                ).fetchone()
                if row is None or str(row["review_status"] or "") != "candidate":
                    continue
                conn.execute(
                    f"DELETE FROM {table} WHERE {primary_key} = ? "
                    "AND instrument_id = ? AND review_status = 'candidate'",
                    (record_id, instrument_id),
                )
                if "business_profile_exceptions" in existing_tables:
                    conn.execute(
                        "DELETE FROM business_profile_exceptions WHERE target_id = ? "
                        "AND instrument_id = ?",
                        (record_id, instrument_id),
                    )
                deleted.append(f"{record_type}:{record_id}")
        return deleted

    def _replay_operating_fact_group(
        self, instrument_id: str, issue: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Split only candidate rows whose persisted evidence proves occurrence identity."""

        changes: list[dict[str, Any]] = []
        for record_id in issue.get("details", {}).get("record_ids") or ():
            record = self.repository.get_record("operating_facts", str(record_id))
            if record is None or record.get("review_status") != "candidate":
                changes.append({
                    "instrument_id": instrument_id,
                    "status": "unchanged",
                    "reason": "approved_history_preserved_or_record_absent",
                    "stable_id": issue["stable_id"],
                    "affected_ids": [str(record_id)],
                })
                continue
            metadata = dict(record.get("metadata") or {})
            identity_material = {
                "record_id": record.get("record_id"),
                "evidence_id": record.get("evidence_id"),
                "exact_evidence": metadata.get("exact_evidence"),
                "value_raw": record.get("value_raw"),
                "unit_raw": record.get("unit_raw"),
            }
            source_row_key = "repair-row-" + hashlib.sha256(
                repr(sorted(identity_material.items())).encode("utf-8")
            ).hexdigest()[:24]
            successor = dict(record)
            successor["record_id"] = "bp-operating-repair-" + hashlib.sha256(
                repr(sorted({**identity_material, "source_row_key": source_row_key}.items())).encode("utf-8")
            ).hexdigest()[:24]
            successor["fact_scope"] = (
                str(record.get("fact_scope") or "").split("#", 1)[0]
                + "#"
                + source_row_key
            )
            successor["review_status"] = "candidate"
            successor_metadata = dict(metadata)
            successor_metadata.update({
                "source_row_key": source_row_key,
                "occurrence_identity_quality": "repaired_from_persisted_evidence",
                "repair_issue_id": issue["stable_id"],
                "repair_origin": "local_replayed",
            })
            successor["metadata"] = successor_metadata
            self.repository.upsert("operating_facts", successor)
            audit = self.review_service.system_hold_candidate(
                "operating_facts",
                str(record_id),
                expected_updated_at=str(record.get("updated_at") or ""),
                reason="legacy broad fact identity replaced by evidence-derived occurrence candidate",
                metadata={"repair_issue_id": issue["stable_id"]},
            )
            changes.append({
                "instrument_id": instrument_id,
                "status": "changed",
                "reason": "operating_fact_occurrence_replayed",
                "stable_id": issue["stable_id"],
                "affected_ids": [str(record_id), successor["record_id"]],
                "review_audit_id": audit.get("audit_id"),
            })
        return changes

    def _hold_invalid_role(
        self, instrument_id: str, issue: dict[str, Any]
    ) -> dict[str, Any]:
        record_id = str(issue.get("details", {}).get("record_id") or "")
        record = self.repository.get_record("value_chain_roles", record_id)
        if record is None:
            return {
                "instrument_id": instrument_id,
                "status": "unchanged",
                "reason": "role_already_absent_or_not_approved",
                "stable_id": issue["stable_id"],
            }
        if record.get("review_status") == "candidate":
            # Candidate roles created by the obsolete inventory rule have no
            # review history or downstream authority and are pure machine
            # garbage.  Remove them instead of leaving another hidden state.
            with self.storage.get_connection() as conn:
                self.storage._apply_pragmas(conn)
                conn.execute("BEGIN IMMEDIATE")
                try:
                    conn.execute(
                        "DELETE FROM company_value_chain_roles WHERE record_id = ? "
                        "AND instrument_id = ? AND review_status = 'candidate'",
                        (record_id, instrument_id),
                    )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
            return {
                "instrument_id": instrument_id,
                "status": "changed",
                "reason": "invalid_candidate_role_deleted",
                "stable_id": issue["stable_id"],
                "affected_ids": [record_id],
            }
        if record.get("review_status") != "approved":
            return {
                "instrument_id": instrument_id,
                "status": "unchanged",
                "reason": "role_not_currently_approved",
                "stable_id": issue["stable_id"],
            }
        audit = self.review_service.system_hold_approved_record(
            "value_chain_roles",
            record_id,
            expected_updated_at=str(record.get("updated_at") or ""),
            reason="role derived from internal inventory, not external storage service",
            metadata={
                "repair_issue_id": issue["stable_id"],
                "preserved_evidence_id": record.get("evidence_id"),
            },
        )
        return {
            "instrument_id": instrument_id,
            "status": "changed",
            "reason": "inventory_derived_storage_role_held",
            "stable_id": issue["stable_id"],
            "affected_ids": [record_id],
            "review_audit_id": audit.get("audit_id"),
        }

    def _hold_unsafe_relationship(
        self, instrument_id: str, issue: dict[str, Any]
    ) -> dict[str, Any]:
        relationship_id = str(issue.get("details", {}).get("relationship_id") or "")
        record = self.repository.get_record("relationships", relationship_id)
        if record is None:
            return {
                "instrument_id": instrument_id,
                "status": "unchanged",
                "reason": "relationship_already_absent",
                "stable_id": issue["stable_id"],
            }
        if record.get("review_status") != "approved":
            return {
                "instrument_id": instrument_id,
                "status": "unchanged",
                "reason": "relationship_no_longer_approved",
                "stable_id": issue["stable_id"],
                "affected_ids": [relationship_id],
            }
        try:
            audit = self.review_service.system_hold_approved_record(
                "relationships",
                relationship_id,
                expected_updated_at=str(record.get("updated_at") or ""),
                reason=(
                    "automatic short-name entity resolution is not supported by "
                    "governed legal-name evidence"
                ),
                metadata={
                    "repair_issue_id": issue["stable_id"],
                    "resolution_basis": issue.get("details", {}).get(
                        "resolution_basis"
                    ),
                    "preserved_evidence_id": record.get("evidence_id"),
                    "required_next_state": "disclosed_name_only_or_governed_successor",
                },
            )
        except ValueError as exc:
            return {
                "instrument_id": instrument_id,
                "status": "held",
                "reason": "relationship_repair_requires_human_review",
                "stable_id": issue["stable_id"],
                "affected_ids": [relationship_id],
                "details": str(exc),
            }
        return {
            "instrument_id": instrument_id,
            "status": "changed",
            "reason": "relationship_short_name_resolution_held_by_review_owner",
            "stable_id": issue["stable_id"],
            "affected_ids": [relationship_id],
            "review_audit_id": audit.get("audit_id"),
        }

    def _controller_provenance_finding(
        self,
        snapshot: dict[str, Any],
        payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Classify only controller values that local evidence can prove unsafe."""

        name = str(snapshot.get("control_owner_name") or "").strip()
        if not name:
            return None
        control_changes = self.storage.list_shareholder_control_changes(
            str(snapshot.get("instrument_id") or "")
        )
        controller_names = {
            str(item.get("actual_controller_name") or "").strip()
            for item in control_changes
        }
        if name in controller_names:
            return None
        ownership_provenance = self._scope_provenance(
            snapshot, payload, "reference_only_ownership_clues"
        )
        source = str(ownership_provenance.get("source") or snapshot.get("source") or "").strip().lower()
        top_holder_names = {
            str(item.get("holder_name") or item.get("name") or "").strip()
            for item in payload.get("top_holders") or ()
            if isinstance(item, dict)
        }
        details = {
            "control_owner_name": name,
            "ownership_source": source or None,
            "top_holder_match": name in top_holder_names,
            "control_history_match": False,
            "scope_provenance": ownership_provenance,
        }
        official_sources = {"cninfo", "official", "sse", "szse", "bse"}
        aggregate_sources = {"efinance", "akshare", "baostock", "pytdx"}
        if source in official_sources:
            return None
        if source in aggregate_sources and name in top_holder_names:
            return {"code": "shareholder_inferred_controller", "details": details}
        return {
            "code": "shareholder_controller_provenance_ambiguous",
            "details": details,
        }

    @staticmethod
    def _scope_provenance(
        snapshot: dict[str, Any],
        payload: dict[str, Any],
        scope: str,
    ) -> dict[str, Any]:
        provenance = payload.get("scope_raw_provenance")
        selected = provenance.get(scope) if isinstance(provenance, dict) else None
        if isinstance(selected, dict):
            return dict(selected)
        scope_sources = payload.get("scope_sources")
        source = scope_sources.get(scope) if isinstance(scope_sources, dict) else None
        if source:
            source_name, _, source_mode = str(source).partition(":")
            return {"source": source_name, "source_mode": source_mode or None}
        return {
            "source": snapshot.get("source"),
            "source_mode": snapshot.get("source_mode"),
        }

    def _rebuild_snapshot(self, snapshot: dict[str, Any], *, clear_inferred_controller: bool) -> ShareholderSnapshot:
        payload = dict(snapshot.get("snapshot") or {})
        self._normalize_snapshot_report_dates(payload)
        ownership = dict(payload.get("ownership_clues") or {})
        if clear_inferred_controller:
            for key in ("control_owner_name", "control_owner_ratio", "direct_controller_name", "control_type", "control_holding_shares"):
                ownership.pop(key, None)
            payload["ownership_clues"] = ownership
        merge_owner = ShareholderShadowSyncService(db_ops=None, storage=self.storage)
        scope_set = actual_shareholder_coverage_scope(
            exchange=str(snapshot.get("exchange") or ""),
            snapshot_json=payload,
            holder_count=snapshot.get("holder_count"),
        )
        rebuilt: ShareholderSnapshot | None = None
        for scope in (
            "holder_count",
            "top10_holders",
            "reference_only_ownership_clues",
        ):
            if scope not in scope_set:
                continue
            incoming = self._scope_snapshot(snapshot, payload, scope)
            rebuilt = merge_owner._merge_snapshots(rebuilt, incoming)
        if rebuilt is None:
            # The snapshot has no reconstructable scope.  Keep an explicit
            # local incomplete record instead of inventing source data.
            return ShareholderSnapshot(
                instrument_id=str(snapshot["instrument_id"]),
                symbol=str(snapshot.get("symbol") or ""),
                exchange=str(snapshot.get("exchange") or ""),
                coverage_status="reference_only",
                schema_version=str(snapshot.get("schema_version") or "shareholders.v1"),
                source=str(snapshot.get("source") or ""),
                source_mode=str(snapshot.get("source_mode") or "direct"),
                snapshot_json={"coverage_scope": []},
                raw_payload={},
            )
        return rebuilt

    @staticmethod
    def _scope_snapshot(
        snapshot: dict[str, Any],
        payload: dict[str, Any],
        scope: str,
    ) -> ShareholderSnapshot:
        """Build one source-attributable scope for the existing merge owner."""
        field_by_scope = {
            "holder_count": "holder_count",
            "top10_holders": "top_holders",
            "reference_only_ownership_clues": "ownership_clues",
        }
        field = field_by_scope[scope]
        provenance = payload.get("scope_raw_provenance")
        selected = provenance.get(scope) if isinstance(provenance, dict) else None
        selected = selected if isinstance(selected, dict) else {}
        source = str(selected.get("source") or snapshot.get("source") or "")
        source_mode = str(selected.get("source_mode") or snapshot.get("source_mode") or "direct")
        scope_payload = {field: payload.get(field)}
        raw_payload = {field: selected.get("payload", payload.get(field))}
        return ShareholderSnapshot(
            instrument_id=str(snapshot["instrument_id"]),
            symbol=str(snapshot.get("symbol") or ""),
            exchange=str(snapshot.get("exchange") or ""),
            coverage_status=str(snapshot.get("coverage_status") or "reference_only"),
            holder_count=snapshot.get("holder_count") if scope == "holder_count" else None,
            holder_count_report_date=(
                normalize_shareholder_report_date(snapshot.get("holder_count_report_date"))
                if scope == "holder_count" else None
            ),
            top_holders_report_date=(
                normalize_shareholder_report_date(snapshot.get("top_holders_report_date"))
                if scope == "top10_holders" else None
            ),
            top_holders_count=snapshot.get("top_holders_count") if scope == "top10_holders" else None,
            top_holders_total_ratio=(
                snapshot.get("top_holders_total_ratio") if scope == "top10_holders" else None
            ),
            control_owner_name=(
                str((payload.get("ownership_clues") or {}).get("control_owner_name") or "").strip() or None
                if scope == "reference_only_ownership_clues" else None
            ),
            control_owner_ratio=(
                (payload.get("ownership_clues") or {}).get("control_owner_ratio")
                if scope == "reference_only_ownership_clues" else None
            ),
            schema_version=str(snapshot.get("schema_version") or "shareholders.v1"),
            source=source,
            source_mode=source_mode,
            snapshot_json=scope_payload,
            raw_payload=raw_payload,
        )

    @staticmethod
    def _normalize_snapshot_report_dates(payload: dict[str, Any]) -> None:
        """Normalize only the persisted shareholder date fields in place."""
        holder = payload.get("holder_count")
        if isinstance(holder, dict):
            holder["report_date"] = normalize_shareholder_report_date(
                holder.get("report_date")
            )
        holders = payload.get("top_holders")
        if isinstance(holders, list):
            for item in holders:
                if isinstance(item, dict):
                    item["report_date"] = normalize_shareholder_report_date(
                        item.get("report_date")
                    )
        ownership = payload.get("ownership_clues")
        if isinstance(ownership, dict):
            ownership["report_date"] = normalize_shareholder_report_date(
                ownership.get("report_date")
            )


    def _is_short_name_resolution(self, record: dict[str, Any]) -> bool:
        entity_id = str(record.get("counterparty_entity_id") or "").strip()
        raw_name = str(record.get("counterparty_name_raw") or "").strip()
        if not entity_id or not raw_name:
            return False
        profile = self.storage.get_company_profile(entity_id, include_snapshot=False)
        return bool(
            profile
            and raw_name == str(profile.get("short_name") or "").strip()
        )

    @staticmethod
    def _relationship_resolution_basis(
        record: dict[str, Any],
        metadata: dict[str, Any],
    ) -> str:
        entity_resolution = metadata.get("entity_resolution")
        nested_basis = (
            entity_resolution.get("basis")
            if isinstance(entity_resolution, dict)
            else None
        )
        return str(
            record.get("resolution_basis")
            or nested_basis
            or metadata.get("resolution_basis")
            or ""
        ).strip()

    def _relationship_lineage_findings(self, instrument_id: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        approved = [item for item in rows if item.get("review_status") == "approved"]
        lineages: dict[tuple[Any, ...], set[str]] = {}
        for item in approved:
            key = tuple(item.get(field) for field in (
                "relationship_type", "counterparty_name_raw", "counterparty_name_normalized", "scope_id", "object_raw", "object_id"
            ))
            lineages.setdefault(key, set()).add(str(item.get("report_period") or ""))
        return [
            self._issue("relationship_multiple_report_cohorts", instrument_id, {"report_periods": sorted(periods)})
            for periods in lineages.values() if len(periods) > 1
        ]

    def _exposure_collision_findings(self, instrument_id: str) -> list[dict[str, Any]]:
        rows = self.repository.list_records(
            "exposures", instrument_id=instrument_id, limit=10000
        )
        by_id = {str(item.get("exposure_id") or ""): item for item in rows}
        issues: list[dict[str, Any]] = []
        for item in rows:
            if item.get("review_status") != "approved":
                continue
            predecessor_id = str(item.get("supersedes_exposure_id") or "").strip()
            predecessor = by_id.get(predecessor_id)
            if predecessor is None:
                continue
            action = str((item.get("metadata") or {}).get("source_activity_action") or "").strip()
            prior_action = str(
                (predecessor.get("metadata") or {}).get("source_activity_action") or ""
            ).strip()
            if not action or not prior_action or action == prior_action:
                continue
            issues.append(
                self._issue(
                    "exposure_action_collision",
                    instrument_id,
                    {
                        "exposure_id": item.get("exposure_id"),
                        "predecessor_exposure_id": predecessor_id,
                        "actions": sorted({action, prior_action}),
                    },
                )
            )
        return issues

    @staticmethod
    def _snapshot_dates(payload: dict[str, Any]) -> list[Any]:
        dates = []
        holder = payload.get("holder_count")
        if isinstance(holder, dict):
            dates.append(holder.get("report_date"))
        dates.extend(item.get("report_date") for item in payload.get("top_holders") or () if isinstance(item, dict))
        ownership = payload.get("ownership_clues")
        if isinstance(ownership, dict):
            dates.append(ownership.get("report_date"))
        return [item for item in dates if item not in (None, "")]

    @staticmethod
    def _issue(code: str, instrument_id: str, details: dict[str, Any]) -> dict[str, Any]:
        digest = hashlib.sha256(
            repr(sorted(details.items())).encode("utf-8")
        ).hexdigest()[:16]
        stable_id = f"{code}:{instrument_id}:{digest}"
        return {"code": code, "instrument_id": instrument_id, "stable_id": stable_id, "details": details}

    @staticmethod
    def _change_counts(changes: list[dict[str, Any]]) -> dict[str, int]:
        counts = Counter(item.get("status") for item in changes)
        return {
            name: int(counts.get(name, 0))
            for name in ("would_change", "changed", "unchanged", "held", "failed")
        }

    def _current_projections(self, instrument_ids: Iterable[str]) -> dict[str, Any]:
        cutoff = str(get_shanghai_time().date())
        output: dict[str, Any] = {}
        for instrument_id in instrument_ids:
            output[str(instrument_id)] = {
                "cutoff": cutoff,
                "relationships": [
                    str(item.get("relationship_id") or "")
                    for item in self.repository.get_approved_as_of(
                        "relationships", instrument_id=str(instrument_id), cutoff=cutoff
                    )
                ],
                "exposures": [
                    str(item.get("exposure_id") or "")
                    for item in self.repository.get_approved_as_of(
                        "exposures", instrument_id=str(instrument_id), cutoff=cutoff
                    )
                ],
            }
        return output

    @staticmethod
    def _report(
        findings: list[dict[str, Any]],
        *,
        apply: bool,
        result_policy: str = "reuse",
        before_projections: dict[str, Any] | None = None,
        execution_state: dict[str, Any] | None = None,
        cleanup_only: bool = False,
    ) -> dict[str, Any]:
        execution_state = execution_state or {
            "work_items": {"delete_count": 0, "reason_counts": {}, "work_ids": []},
            "checkpoint_files": {
                "enabled": False,
                "root": None,
                "delete_paths": [],
                "orphan_paths": [],
                "unsafe_paths": [],
                "delete_bytes": 0,
            },
            "issues": [],
        }
        checkpoint_files = dict(execution_state.get("checkpoint_files") or {})
        public_checkpoint_files = {
            **checkpoint_files,
            **BusinessProfileSemanticRepairService._id_summary(
                checkpoint_files.get("delete_paths") or (), key="delete_paths"
            ),
            **BusinessProfileSemanticRepairService._id_summary(
                checkpoint_files.get("orphan_paths") or (), key="orphan_paths"
            ),
        }
        public_execution_state = {
            **execution_state,
            "work_items": {
                key: value
                for key, value in dict(execution_state.get("work_items") or {}).items()
                if not str(key).startswith("_")
            },
            "checkpoint_files": public_checkpoint_files,
        }
        issues = [
            *[item for finding in findings for item in finding["issues"]],
            *list(execution_state.get("issues") or []),
        ]
        would_change = sum(
            item["code"]
            in {
                "shareholder_scope_mismatch",
                "shareholder_noncanonical_report_date",
                "shareholder_inferred_controller",
                "relationship_short_name_auto_resolution",
                "inventory_derived_storage_role",
                "incompatible_reusable_artifact",
                "legacy_semantic_artifact",
                "legacy_shadow_semantic_artifact",
                "failed_semantic_artifact",
                "legacy_shadow_semantic_run",
                "superseded_semantic_run",
                "obsolete_work_items",
                "orphan_checkpoint_files",
            }
            for item in issues
        )
        return {
            "schema_version": REPAIR_SCHEMA_VERSION,
            "mode": "apply" if apply else "audit",
            "result_policy": result_policy,
            "cleanup_only": cleanup_only,
            "network_access": False,
            "llm_access": False,
            "write_count": 0,
            "instruments": findings,
            "execution_state": public_execution_state,
            "issue_counts": dict(Counter(item["code"] for item in issues)),
            "before_current_projections": before_projections or {},
            "after_current_projections": before_projections or {},
            "change_counts": {
                "would_change": 0 if apply else would_change,
                "changed": 0,
                "unchanged": 0,
                "held": 0,
                "failed": 0,
            },
        }
