"""Persisted readiness projections for shared announcement assets."""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .capacity_artifact import CapacityArtifactNotReadyError, validate_capacity_artifact
from .config import AnnouncementAssetConfig
from .repository import AnnouncementAssetRepository
from .storage import ContentAddressedBlobStore


@dataclass(frozen=True)
class ReadinessThresholds:
    stale_heartbeat_seconds: int = 1800
    max_consecutive_failures: int = 3
    max_cursor_lag_seconds: int = 3 * 24 * 3600
    max_retry_age_seconds: int = 7 * 24 * 3600


@dataclass(frozen=True)
class AnnouncementAssetReadinessReport:
    status: str
    ready_for_reads: bool
    ready_for_daily: bool
    ready_for_deletion: bool
    generated_at: str
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    legacy_write_stop_allowed: bool = False
    duplicate_cleanup_allowed: bool = False
    summary: dict[str, Any] = field(default_factory=dict)
    operator_diagnostics: dict[str, Any] | None = None
    schema_version: str = "official_asset_readiness_report.v1"
    config_fingerprint: str = ""
    report_id: str | None = None


class AnnouncementAssetReadinessService:
    """Build redacted and operator projections from durable catalog state."""

    def __init__(
        self,
        *,
        repository: AnnouncementAssetRepository,
        config: AnnouncementAssetConfig,
        thresholds: ReadinessThresholds | None = None,
    ) -> None:
        self.repository = repository
        self.config = config
        self.thresholds = thresholds or ReadinessThresholds()

    def report(
        self,
        *,
        operator: bool = False,
        now: str | None = None,
        persist: bool = False,
        operation_id: str | None = None,
        scope_key: str = "global",
    ) -> AnnouncementAssetReadinessReport:
        timestamp = _parse_time(now) if now else datetime.now(timezone.utc)
        generated_at = timestamp.isoformat()
        metrics = self._metrics(timestamp)
        blockers: list[str] = []
        warnings: list[str] = []
        rollout = self.config.rollout

        if not self.config.enabled:
            blockers.append("module_disabled")
        if self.config.dry_run:
            blockers.append("dry_run_enabled")
        try:
            validate_capacity_artifact(self.config)
        except (CapacityArtifactNotReadyError, OSError, RuntimeError):
            blockers.append("capacity_artifact_not_ready")
        if rollout.require_bootstrap and not metrics["bootstrap_complete"]:
            blockers.append("bootstrap_incomplete")
        if metrics["eligibility_indeterminate"]:
            blockers.append("universe_denominator_indeterminate")
        if rollout.require_bootstrap and not metrics["full_market_universe_complete"]:
            blockers.append("full_market_census_reconciliation_incomplete")
        if rollout.require_integrity and metrics["invalid_effective_blobs"]:
            blockers.append("effective_blob_integrity_failure")
        if metrics["discovery_gap_scopes"]:
            blockers.append("discovery_gaps_present")
        if rollout.require_backup and metrics["unprotected_effective_blobs"]:
            blockers.append("effective_blobs_unprotected")
        if metrics["stale_active_operations"]:
            blockers.append("stale_operation_heartbeat")
        if metrics["consecutive_failures"] >= self.thresholds.max_consecutive_failures:
            blockers.append("scheduler_consecutive_failures")
        if metrics["overdue_missing"]:
            if self.config.overdue_missing_readiness_policy == "blocked":
                blockers.append("overdue_expected_period_missing")
            else:
                warnings.append("overdue_expected_period_missing")
        if metrics["attachment_retry_backlog"]:
            warnings.append("attachment_retry_backlog")
        if metrics["backup_stale"]:
            warnings.append("backup_freshness_degraded")
        if metrics["artifact_sidecar_invalid"]:
            blockers.append("artifact_sidecar_invalid")
        if metrics["part_hard_threshold_crossed"]:
            blockers.append("temporary_part_threshold_exceeded")
        elif metrics["part_warning_threshold_crossed"]:
            warnings.append("temporary_part_threshold_warning")
        if metrics["quarantine_hard_threshold_crossed"]:
            blockers.append("quarantine_hard_threshold_exceeded")
        elif metrics["quarantine_warning_threshold_crossed"]:
            warnings.append("quarantine_warning_threshold_exceeded")
        if metrics["predecessor_cleanup_warning_crossed"]:
            warnings.append("predecessor_cleanup_overdue")
        if rollout.require_storage and metrics["storage_state"] != "available":
            blockers.append("storage_unavailable")
        elif metrics["storage_hard_threshold_crossed"]:
            blockers.append("storage_hard_threshold_exceeded")
        elif metrics["storage_warning_threshold_crossed"]:
            warnings.append("storage_warning_threshold_exceeded")

        read_blockers = {
            "module_disabled",
            "effective_blob_integrity_failure",
        }
        deletion_blockers = set(blockers) | (
            {"backup_freshness_degraded"} if metrics["backup_stale"] else set()
        )
        # Consumer migration is deliberately not part of asset-scheduler
        # readiness.  Cleanup is different: disabling the rollout gate must
        # never turn missing cutover evidence into cleanup authorization.
        consumer_cleanup_blocked = not metrics["consumer_migration_complete"]
        cleanup_hard_blocked = bool(metrics["predecessor_cleanup_hard_crossed"])
        cleanup_handoff_blocked = not all(
            (
                metrics["shared_custody_adoption_complete"],
                metrics["recovery_pair_closure_complete"],
                metrics["cleanup_alias_pins_released"],
            )
        )
        legacy_write_stop_allowed = (
            not consumer_cleanup_blocked
            and not cleanup_handoff_blocked
            and not cleanup_hard_blocked
            and not bool(deletion_blockers)
        )
        duplicate_cleanup_allowed = legacy_write_stop_allowed and not bool(
            metrics["active_operations"]
        ) and not metrics["unresolved_predecessor_count"]
        summary = {
            key: metrics[key]
            for key in (
                "active_universe_size",
                "full_market_universe_complete",
                "paired_census_snapshot_id",
                "census_reconciliation_status",
                "available_assets",
                "overdue_missing",
                "discovery_gap_scopes",
                "attachment_retry_backlog",
                "invalid_effective_blobs",
                "unprotected_effective_blobs",
                "active_operations",
                "last_successful_cutoff",
                "backup_last_verified_at",
                "part_count",
                "part_bytes",
                "oldest_part_age_seconds",
                "part_warning_threshold_crossed",
                "part_hard_threshold_crossed",
                "part_invalid_sidecar_count",
                "quarantine_count",
                "quarantine_bytes",
                "oldest_quarantine_age_seconds",
                "quarantine_invalid_sidecar_count",
                "consumer_migration_complete",
                "consumer_migration_status",
                "consumer_migration_required_asset_count",
                "consumer_migration_missing_asset_count",
                "shared_custody_adoption_complete",
                "consumed_adoption_gate_count",
                "pending_adoption_gate_count",
                "invalidated_adoption_gate_count",
                "adopted_asset_without_consumed_gate_count",
                "recovery_pair_closure_complete",
                "recovery_manifest_count",
                "unclosed_recovery_pair_count",
                "cleanup_alias_pins_released",
                "cleanup_alias_pin_count",
                "active_cleanup_alias_pin_count",
                "missing_attachment_count",
                "estimated_required_bytes",
                "estimate_state",
                "estimate_basis",
                "estimate_as_of",
                "configuration_fingerprint",
                "known_content_length_bytes",
                "unknown_length_reservation_bytes",
                "temporary_publication_overhead_bytes",
                "replacement_peak_bytes",
                "active_storage_reservation_planned_bytes",
                "active_storage_reservation_actual_bytes",
                "unprotected_bytes",
                "storage_state",
                "storage_total_bytes",
                "storage_used_bytes",
                "storage_free_bytes",
                "storage_projected_free_bytes",
                "storage_projected_utilization",
                "storage_warning_threshold_crossed",
                "storage_hard_threshold_crossed",
                "scheduler_enabled",
                "scheduler_last_status",
                "oldest_retry_age_seconds",
                "oldest_unresolved_predecessor_age_seconds",
                "unresolved_predecessor_count",
                "predecessor_cleanup_warning_crossed",
                "predecessor_cleanup_hard_crossed",
            )
        }
        summary["legacy_write_stop_allowed"] = legacy_write_stop_allowed
        summary["duplicate_cleanup_allowed"] = duplicate_cleanup_allowed
        summary["unique_storage_completion_allowed"] = not bool(
            metrics["unresolved_predecessor_count"]
        )
        summary["alerts"] = list(metrics["alerts"])
        report = AnnouncementAssetReadinessReport(
            status="blocked" if blockers else "degraded" if warnings else "ready",
            ready_for_reads=not any(item in read_blockers for item in blockers),
            ready_for_daily=not blockers,
            ready_for_deletion=(
                not deletion_blockers
                and not cleanup_hard_blocked
            ),
            generated_at=generated_at,
            blockers=tuple(sorted(set(blockers))),
            warnings=tuple(sorted(set(warnings))),
            legacy_write_stop_allowed=legacy_write_stop_allowed,
            duplicate_cleanup_allowed=duplicate_cleanup_allowed,
            summary=summary,
            operator_diagnostics=metrics if operator else None,
            config_fingerprint=self.config.config_fingerprint,
        )
        if not persist:
            return report
        stored = self.repository.persist_operational_report(
            report_kind="readiness",
            schema_version=report.schema_version,
            config_fingerprint=report.config_fingerprint,
            status=report.status,
            generated_at=report.generated_at,
            operation_id=operation_id,
            scope_key=scope_key,
            payload={
                "status": report.status,
                "ready_for_reads": report.ready_for_reads,
                "ready_for_daily": report.ready_for_daily,
                "ready_for_deletion": report.ready_for_deletion,
                "blockers": report.blockers,
                "warnings": report.warnings,
                "legacy_write_stop_allowed": report.legacy_write_stop_allowed,
                "duplicate_cleanup_allowed": report.duplicate_cleanup_allowed,
                "summary": report.summary,
                "operator_diagnostics": report.operator_diagnostics,
            },
        )
        return AnnouncementAssetReadinessReport(
            **{
                **report.__dict__,
                "report_id": str(stored["report_id"]),
            }
        )

    def load_latest_report(
        self,
        *,
        operator: bool = False,
        scope_key: str = "global",
    ) -> AnnouncementAssetReadinessReport | None:
        """Reload the latest persisted readiness report as a typed projection."""

        rows = self.repository.list_operational_reports(
            report_kind="readiness",
            scope_key=scope_key,
            limit=1,
        )
        if not rows:
            return None
        stored = rows[0]
        if stored["schema_version"] != "official_asset_readiness_report.v1":
            raise ValueError("unsupported persisted readiness report schema")
        payload = dict(stored["payload"])
        required = {
            "status",
            "ready_for_reads",
            "ready_for_daily",
            "ready_for_deletion",
            "blockers",
            "warnings",
            "legacy_write_stop_allowed",
            "duplicate_cleanup_allowed",
            "summary",
        }
        missing = sorted(required - set(payload))
        if missing:
            raise ValueError(
                "persisted readiness report is missing fields: " + ", ".join(missing)
            )
        return AnnouncementAssetReadinessReport(
            status=str(payload["status"]),
            ready_for_reads=bool(payload["ready_for_reads"]),
            ready_for_daily=bool(payload["ready_for_daily"]),
            ready_for_deletion=bool(payload["ready_for_deletion"]),
            generated_at=str(stored["generated_at"]),
            blockers=tuple(str(item) for item in payload["blockers"]),
            warnings=tuple(str(item) for item in payload["warnings"]),
            legacy_write_stop_allowed=bool(payload["legacy_write_stop_allowed"]),
            duplicate_cleanup_allowed=bool(payload["duplicate_cleanup_allowed"]),
            summary=dict(payload["summary"]),
            operator_diagnostics=(
                dict(payload.get("operator_diagnostics") or {}) if operator else None
            ),
            schema_version=str(stored["schema_version"]),
            config_fingerprint=str(stored["config_fingerprint"]),
            report_id=str(stored["report_id"]),
        )

    def _metrics(self, now: datetime) -> dict[str, Any]:
        artifact_store = ContentAddressedBlobStore(self.config)
        artifact_metrics = artifact_store.artifact_metrics(now=now)
        artifact_evidence = artifact_store.artifact_evidence(now=now)
        storage = self.config.storage
        storage_state = "available"
        storage_error: str | None = None
        storage_total = storage_used = storage_free = 0
        try:
            artifact_store.validate_mount()
            usage_path = self.config.filings_root
            while not usage_path.exists() and usage_path != usage_path.parent:
                usage_path = usage_path.parent
            usage = shutil.disk_usage(usage_path)
            storage_total = int(usage.total)
            storage_used = int(usage.used)
            storage_free = int(usage.free)
        except (OSError, RuntimeError) as exc:
            storage_state = "unavailable"
            storage_error = str(exc)
        with self.repository.connection() as conn:
            latest_snapshot = conn.execute(
                """SELECT * FROM official_asset_universe_snapshots
                   ORDER BY snapshot_at DESC LIMIT 1"""
            ).fetchone()
            coverage = conn.execute(
                """SELECT status, evidence_json FROM official_asset_coverage
                   WHERE universe_snapshot_id=COALESCE(?, universe_snapshot_id)""",
                (None if latest_snapshot is None else latest_snapshot["snapshot_id"],),
            ).fetchall()
            effective = conn.execute(
                """SELECT e.asset_id, e.instrument_id, e.fiscal_year,
                          e.availability, e.content_hash,
                          e.pending_candidate_id, e.visibility_state,
                          b.integrity_status, b.content_length,
                          b.acquisition_origin, b.adopted_from_path,
                          COALESCE(bs.status, b.backup_status) AS backup_status
                   FROM effective_annual_reports e
                   LEFT JOIN official_document_blobs b
                     ON b.content_hash=e.content_hash
                   LEFT JOIN official_asset_backup_state bs
                     ON bs.content_hash=e.content_hash"""
            ).fetchall()
            discovery = conn.execute(
                """SELECT source, exchange, scope_key, status, is_complete,
                          covered_until, gap_reason
                   FROM official_asset_discovery_state"""
            ).fetchall()
            operations = conn.execute(
                """SELECT operation_id, status, operation_type, heartbeat_at,
                          updated_at, finished_at
                   FROM official_asset_operations ORDER BY created_at DESC"""
            ).fetchall()
            retry_count = int(
                conn.execute(
                    """SELECT COUNT(*) FROM official_asset_attachment_retries
                       WHERE status IN ('queued', 'retryable', 'running',
                                        'blocked', 'exhausted', 'operator_hold')"""
                ).fetchone()[0]
            )
            backup = conn.execute(
                "SELECT status, verified_at FROM official_asset_backup_state"
            ).fetchall()
            consumer = conn.execute(
                """SELECT asset_id, consumer, status
                   FROM official_asset_consumer_processing"""
            ).fetchall()
            consumer_asset_scope_rows = conn.execute(
                """SELECT effective.asset_id, pin.owner AS consumer
                   FROM official_asset_retention_pins pin
                   JOIN effective_annual_reports effective
                     ON effective.content_hash=pin.blob_hash
                    AND json_extract(
                            effective.decision_evidence_json,
                            '$.source_file_id'
                        ) = json_extract(pin.metadata_json, '$.source_file_id')
                   WHERE pin.pin_type IN ('legacy_alias', 'managed_alias')
                     AND pin.owner IN ('business_profile', 'broker_risk_control')
                   UNION
                   SELECT gate.asset_id,
                          json_extract(
                              gate.evidence_json,
                              '$.legacy_custody_evidence.consumer'
                          ) AS consumer
                   FROM official_asset_adoption_promotion_gates gate
                   WHERE json_extract(
                             gate.evidence_json,
                             '$.legacy_custody_evidence.consumer'
                         ) IN ('business_profile', 'broker_risk_control')
                   UNION
                   SELECT asset_id, consumer
                   FROM official_asset_consumer_processing
                   WHERE consumer IN ('business_profile', 'broker_risk_control')
                     AND status IN ('current', 'failed')"""
            ).fetchall()
            attachments = conn.execute(
                """SELECT a.attachment_id, a.content_length_hint
                   FROM official_announcement_attachments a
                   LEFT JOIN official_attachment_versions v
                     ON v.attachment_id=a.attachment_id
                    AND v.integrity_status='valid'
                    AND v.content_hash IS NOT NULL
                   WHERE v.version_id IS NULL"""
            ).fetchall()
            reservations = conn.execute(
                """SELECT planned_bytes, actual_bytes
                   FROM official_asset_storage_reservations
                   WHERE status='active'"""
            ).fetchall()
            deletions = conn.execute(
                """SELECT deletion_id, status, planned_at, error_code,
                          recovery_pin_id, recovery_manifest_id
                   FROM official_asset_deletion_intents
                   WHERE status IN ('planned', 'deleting', 'failed')
                   ORDER BY planned_at"""
            ).fetchall()
            adoption_gates = conn.execute(
                """SELECT gate_id, asset_id, status
                   FROM official_asset_adoption_promotion_gates"""
            ).fetchall()
            adopted_without_consumed_gate = int(
                conn.execute(
                    """SELECT COUNT(*)
                       FROM effective_annual_reports effective
                       JOIN official_document_blobs blob
                         ON blob.content_hash=effective.content_hash
                       WHERE effective.visibility_state='production'
                         AND (blob.adopted_from_path IS NOT NULL
                              OR LOWER(COALESCE(blob.acquisition_origin, ''))
                                 LIKE 'adopt%')
                         AND NOT EXISTS (
                             SELECT 1
                             FROM official_asset_adoption_promotion_gates gate
                             WHERE gate.asset_id=effective.asset_id
                               AND gate.content_hash=effective.content_hash
                               AND gate.status='consumed'
                         )"""
                ).fetchone()[0]
            )
            recovery_pair_counts = conn.execute(
                """SELECT COUNT(*) AS manifest_count,
                          SUM(CASE WHEN closure.closure_id IS NULL THEN 1 ELSE 0 END)
                              AS unclosed_count
                   FROM official_asset_recovery_manifest manifest
                   LEFT JOIN official_asset_recovery_pair_closures closure
                     ON closure.recovery_pair_id=manifest.recovery_pair_id
                    AND closure.recovery_id=manifest.recovery_id"""
            ).fetchone()
            cleanup_alias_pin_counts = conn.execute(
                """SELECT COUNT(*) AS pin_count,
                          SUM(CASE WHEN released_at IS NULL THEN 1 ELSE 0 END)
                              AS active_count
                   FROM official_asset_retention_pins
                   WHERE pin_type IN ('legacy_alias', 'managed_alias')"""
            ).fetchone()

        latest_snapshot_payload = (
            None
            if latest_snapshot is None
            else self.repository.get_universe_snapshot(
                str(latest_snapshot["snapshot_id"])
            )
        )
        latest_metadata = (
            latest_snapshot_payload.get("metadata", {})
            if isinstance(latest_snapshot_payload, dict)
            else {}
        )
        reconciliation = (
            latest_metadata.get("census_reconciliation")
            if isinstance(latest_metadata, dict)
            else None
        )
        census_reconciliation_status = (
            str(reconciliation.get("status"))
            if isinstance(reconciliation, dict) and reconciliation.get("status")
            else None
        )
        paired_census_snapshot_id = (
            None
            if latest_snapshot_payload is None
            else latest_snapshot_payload.get("paired_census_snapshot_id")
        )

        active_universe_size = 0 if latest_snapshot is None else int(
            latest_snapshot["eligible_count"] or 0
        )
        eligibility_indeterminate = 0 if latest_snapshot is None else int(
            latest_snapshot["indeterminate_count"] or 0
        )
        invalid_effective = sum(
            1
            for row in effective
            if row["content_hash"]
            and row["integrity_status"] not in {"valid"}
        )
        unprotected_effective = sum(
            1
            for row in effective
            if row["content_hash"] and row["backup_status"] != "verified"
        )
        incomplete_coverage = sum(
            1 for row in coverage if row["status"] in {"incomplete", "retryable", "blocked"}
        )
        overdue_missing = sum(
            1 for row in coverage if "overdue_missing" in str(row["evidence_json"] or "")
        )
        stale_active = 0
        for row in operations:
            if row["status"] not in {"queued", "running"}:
                continue
            heartbeat = _parse_time(row["heartbeat_at"] or row["updated_at"])
            if (now - heartbeat).total_seconds() > self.thresholds.stale_heartbeat_seconds:
                stale_active += 1
        consecutive_failures = 0
        for row in operations:
            if row["status"] in {"failed", "blocked"}:
                consecutive_failures += 1
            elif row["status"] == "completed":
                break
        backup_times = [
            _parse_time(row["verified_at"])
            for row in backup
            if row["status"] == "verified" and row["verified_at"]
        ]
        latest_backup = max(backup_times) if backup_times else None
        backup_stale = bool(
            effective
            and (
                latest_backup is None
                or (now - latest_backup).total_seconds()
                > self.config.backup.freshness_hours * 3600
            )
        )
        consumer_status: dict[str, str] = {}
        current_consumer_assets: dict[str, set[str]] = {}
        for row in consumer:
            canonical_consumer = _canonical_consumer_name(str(row["consumer"]))
            status = str(row["status"])
            if status == "current" or canonical_consumer not in consumer_status:
                consumer_status[canonical_consumer] = status
            if status == "current":
                current_consumer_assets.setdefault(canonical_consumer, set()).add(
                    str(row["asset_id"])
                )
        production_consumer_assets = {
            str(row["asset_id"])
            for row in effective
            if row["visibility_state"] == "production"
            and row["availability"] == "local_valid"
        }
        required_consumers = ("business_profile", "broker_risk_control")
        explicit_consumer_asset_scope: dict[str, set[str]] = {}
        for row in consumer_asset_scope_rows:
            consumer_name = _canonical_consumer_name(str(row["consumer"] or ""))
            if consumer_name not in required_consumers:
                continue
            explicit_consumer_asset_scope.setdefault(consumer_name, set()).add(
                str(row["asset_id"])
            )
        required_consumer_assets = {
            name: (
                explicit_consumer_asset_scope.get(name, set())
                & production_consumer_assets
                if explicit_consumer_asset_scope.get(name)
                else set(production_consumer_assets)
            )
            for name in required_consumers
        }
        consumer_missing_asset_count = {
            name: len(
                required_consumer_assets[name]
                - current_consumer_assets.get(name, set())
            )
            for name in required_consumers
        }
        migration_complete = bool(production_consumer_assets) and all(
            required_consumer_assets[name]
            and consumer_missing_asset_count[name] == 0
            for name in required_consumers
        )
        complete_cutoffs = [
            row["covered_until"]
            for row in discovery
            if row["is_complete"] and row["covered_until"]
        ]
        cursor_lag_seconds = {
            "/".join(
                (str(row["source"]), str(row["exchange"]), str(row["scope_key"]))
            ): max(
                0.0,
                (now - _parse_time(row["covered_until"])).total_seconds(),
            )
            for row in discovery
            if row["covered_until"]
        }
        available_coverage = sum(
            1 for row in coverage if row["status"] == "available"
        )
        missing_attachment_count = max(
            0,
            active_universe_size - available_coverage,
            sum(1 for row in coverage if row["status"] != "available"),
        )
        pending_hints = [
            int(row["content_length_hint"])
            for row in attachments
            if row["content_length_hint"] is not None
            and int(row["content_length_hint"]) > 0
        ]
        known_count = min(missing_attachment_count, len(pending_hints))
        # Attachment rows are not yet bound one-to-one to denominator gaps.
        # Selecting the largest known hints avoids presenting an optimistic
        # capacity figure when more hints exist than missing instruments.
        known_content_length_bytes = sum(
            sorted(pending_hints, reverse=True)[:known_count]
        )
        unknown_count = max(0, missing_attachment_count - known_count)
        unknown_length_reservation_bytes = (
            unknown_count * self.config.storage.unknown_length_reservation_bytes
        )
        publication_bytes = (
            known_content_length_bytes + unknown_length_reservation_bytes
        )
        pending_replacement_rows = [
            row
            for row in effective
            if row["content_hash"] and row["pending_candidate_id"]
        ]
        replacement_peak_bytes = sum(
            int(row["content_length"] or 0)
            + self.config.storage.unknown_length_reservation_bytes
            for row in pending_replacement_rows
        )
        snapshot_complete = bool(
            latest_snapshot is not None
            and str(latest_snapshot["status"]) == "complete"
            and latest_snapshot["source_complete"]
            and not eligibility_indeterminate
            and paired_census_snapshot_id
            and census_reconciliation_status == "complete"
        )
        snapshot_fresh = bool(
            latest_snapshot is not None
            and (
                now - _parse_time(str(latest_snapshot["snapshot_at"]))
            ).total_seconds()
            <= int(latest_snapshot["freshness_limit_seconds"])
        )
        estimate_state = (
            "unavailable"
            if latest_snapshot is None
            else "indeterminate"
            if not snapshot_complete or not snapshot_fresh
            else "available"
        )
        estimated_required_bytes = (
            None
            if estimate_state != "available"
            else publication_bytes + publication_bytes + replacement_peak_bytes
        )
        retry_first_times = [
            _parse_time(str(item["first_queued_at"]))
            for item in self.repository.list_attachment_retries(limit=1000)
            if item.get("first_queued_at")
        ]
        oldest_retry_age = (
            None
            if not retry_first_times
            else max(0.0, (now - min(retry_first_times)).total_seconds())
        )
        predecessor_ages = [
            max(0.0, (now - _parse_time(row["planned_at"])).total_seconds())
            for row in deletions
        ]
        oldest_predecessor_index = (
            None
            if not predecessor_ages
            else max(range(len(predecessor_ages)), key=predecessor_ages.__getitem__)
        )
        oldest_predecessor = (
            None if oldest_predecessor_index is None else deletions[oldest_predecessor_index]
        )
        oldest_predecessor_age = (
            0.0 if oldest_predecessor_index is None else predecessor_ages[oldest_predecessor_index]
        )
        scheduler_operations = [
            row
            for row in operations
            if row["operation_type"]
            in {
                "annual_report_asset_latest_backfill",
                "annual_report_asset_daily_update",
                "annual_report_asset_integrity_audit",
                "annual_report_asset_backup",
            }
        ]
        unprotected_by_hash = {
            str(row["content_hash"]): int(row["content_length"] or 0)
            for row in effective
            if row["content_hash"] and row["backup_status"] != "verified"
        }
        unprotected_bytes = sum(unprotected_by_hash.values())
        active_reservation_planned = sum(
            int(row["planned_bytes"] or 0) for row in reservations
        )
        active_reservation_actual = sum(
            int(row["actual_bytes"] or 0) for row in reservations
        )
        projected_increment = active_reservation_planned + int(
            estimated_required_bytes or 0
        )
        projected_used = storage_used + projected_increment
        projected_free = storage_free - projected_increment
        projected_utilization = (
            None
            if not storage_total
            else projected_used / max(storage_total, 1)
        )
        alerts: list[str] = []
        if stale_active:
            alerts.append("stale_heartbeat")
        if consecutive_failures >= self.thresholds.max_consecutive_failures:
            alerts.append("consecutive_failures")
        if any(
            lag > self.thresholds.max_cursor_lag_seconds
            for lag in cursor_lag_seconds.values()
        ):
            alerts.append("cursor_lag")
        if (
            oldest_retry_age is not None
            and oldest_retry_age > self.thresholds.max_retry_age_seconds
        ):
            alerts.append("oldest_retry_age")
        if backup_stale:
            alerts.append("backup_freshness")
        return {
            "active_universe_size": active_universe_size,
            "universe_snapshot_status": None if latest_snapshot is None else latest_snapshot["status"],
            "full_market_universe_complete": snapshot_complete,
            "paired_census_snapshot_id": paired_census_snapshot_id,
            "census_reconciliation_status": census_reconciliation_status,
            "eligibility_indeterminate": eligibility_indeterminate,
            "bootstrap_complete": bool(
                snapshot_complete
                and not incomplete_coverage
            ),
            "available_assets": sum(1 for row in effective if row["availability"] == "local_valid"),
            "overdue_missing": overdue_missing,
            "incomplete_coverage": incomplete_coverage,
            "discovery_gap_scopes": sum(
                1 for row in discovery if not row["is_complete"] or row["gap_reason"]
            ),
            "last_successful_cutoff": max(complete_cutoffs) if complete_cutoffs else None,
            "cursor_lag_seconds": cursor_lag_seconds,
            "attachment_retry_backlog": retry_count,
            "oldest_retry_age_seconds": oldest_retry_age,
            "invalid_effective_blobs": invalid_effective,
            "unprotected_effective_blobs": unprotected_effective,
            "active_operations": sum(
                1 for row in operations if row["status"] in {"queued", "running"}
            ),
            "stale_active_operations": stale_active,
            "consecutive_failures": consecutive_failures,
            "backup_last_verified_at": None if latest_backup is None else latest_backup.isoformat(),
            "backup_stale": backup_stale,
            "consumer_migration_complete": migration_complete,
            "consumer_migration_status": consumer_status,
            "consumer_migration_required_asset_count": {
                name: len(required_consumer_assets[name])
                for name in required_consumers
            },
            "consumer_migration_missing_asset_count": consumer_missing_asset_count,
            "shared_custody_adoption_complete": bool(
                any(row["status"] == "consumed" for row in adoption_gates)
                and not any(row["status"] == "ready" for row in adoption_gates)
                and adopted_without_consumed_gate == 0
            ),
            "consumed_adoption_gate_count": sum(
                1 for row in adoption_gates if row["status"] == "consumed"
            ),
            "pending_adoption_gate_count": sum(
                1 for row in adoption_gates if row["status"] == "ready"
            ),
            "invalidated_adoption_gate_count": sum(
                1 for row in adoption_gates if row["status"] == "invalidated"
            ),
            "adopted_asset_without_consumed_gate_count": adopted_without_consumed_gate,
            "recovery_pair_closure_complete": bool(
                int(recovery_pair_counts["manifest_count"] or 0) > 0
                and int(recovery_pair_counts["unclosed_count"] or 0) == 0
            ),
            "recovery_manifest_count": int(
                recovery_pair_counts["manifest_count"] or 0
            ),
            "unclosed_recovery_pair_count": int(
                recovery_pair_counts["unclosed_count"] or 0
            ),
            "cleanup_alias_pins_released": bool(
                int(cleanup_alias_pin_counts["pin_count"] or 0) > 0
                and int(cleanup_alias_pin_counts["active_count"] or 0) == 0
            ),
            "cleanup_alias_pin_count": int(
                cleanup_alias_pin_counts["pin_count"] or 0
            ),
            "active_cleanup_alias_pin_count": int(
                cleanup_alias_pin_counts["active_count"] or 0
            ),
            "missing_attachment_count": missing_attachment_count,
            "estimated_required_bytes": estimated_required_bytes,
            "estimate_state": estimate_state,
            "estimate_basis": "largest_content_length_hints_or_unknown_reservation.v2",
            "estimate_as_of": now.isoformat(),
            "configuration_fingerprint": self.config.config_fingerprint,
            "known_content_length_bytes": known_content_length_bytes,
            "unknown_length_reservation_bytes": unknown_length_reservation_bytes,
            "temporary_publication_overhead_bytes": publication_bytes,
            "replacement_peak_bytes": replacement_peak_bytes,
            "active_storage_reservation_planned_bytes": active_reservation_planned,
            "active_storage_reservation_actual_bytes": active_reservation_actual,
            "unprotected_bytes": unprotected_bytes,
            "storage_state": storage_state,
            "storage_total_bytes": storage_total,
            "storage_used_bytes": storage_used,
            "storage_free_bytes": storage_free,
            "storage_projected_free_bytes": projected_free,
            "storage_utilization": (
                None
                if not storage_total
                else storage_used / max(storage_total, 1)
            ),
            "storage_projected_utilization": projected_utilization,
            "storage_warning_threshold_crossed": bool(
                projected_utilization is not None
                and projected_utilization >= storage.warning_utilization
            ),
            "storage_hard_threshold_crossed": bool(
                projected_utilization is not None
                and (
                    projected_utilization >= storage.hard_stop_utilization
                    or projected_free < storage.free_space_reserve_bytes
                )
            ),
            "storage_error": storage_error,
            "scheduler_enabled": bool(
                self.config.enabled and self.config.scheduled_enabled
            ),
            "scheduler_last_status": (
                None if not scheduler_operations else scheduler_operations[0]["status"]
            ),
            "recent_runs": tuple(
                {
                    "operation_id": str(row["operation_id"]),
                    "operation_type": str(row["operation_type"]),
                    "status": str(row["status"]),
                    "heartbeat_at": row["heartbeat_at"],
                    "updated_at": row["updated_at"],
                    "finished_at": row["finished_at"],
                }
                for row in scheduler_operations[:30]
            ),
            "alert_thresholds": {
                "stale_heartbeat_seconds": self.thresholds.stale_heartbeat_seconds,
                "max_consecutive_failures": self.thresholds.max_consecutive_failures,
                "max_cursor_lag_seconds": self.thresholds.max_cursor_lag_seconds,
                "max_retry_age_seconds": self.thresholds.max_retry_age_seconds,
            },
            "alerts": tuple(alerts),
            "unresolved_predecessor_count": len(deletions),
            "oldest_unresolved_predecessor_age_seconds": oldest_predecessor_age,
            "oldest_unresolved_predecessor_state": (
                None if oldest_predecessor is None else oldest_predecessor["status"]
            ),
            "oldest_unresolved_predecessor_error": (
                None if oldest_predecessor is None else oldest_predecessor["error_code"]
            ),
            "predecessor_cleanup_warning_age_seconds": (
                storage.predecessor_cleanup_warning_age_seconds
            ),
            "predecessor_cleanup_hard_age_seconds": (
                storage.predecessor_cleanup_hard_age_seconds
            ),
            "predecessor_cleanup_warning_crossed": bool(
                predecessor_ages
                and oldest_predecessor_age
                >= storage.predecessor_cleanup_warning_age_seconds
            ),
            "predecessor_cleanup_hard_crossed": bool(
                predecessor_ages
                and oldest_predecessor_age
                >= storage.predecessor_cleanup_hard_age_seconds
            ),
            "predecessor_operator_repair_disposition": (
                "operator_repair_required"
                if predecessor_ages
                and oldest_predecessor_age
                >= storage.predecessor_cleanup_hard_age_seconds
                else "monitor"
                if predecessor_ages
                and oldest_predecessor_age
                >= storage.predecessor_cleanup_warning_age_seconds
                else "not_due"
            ),
            "part_count": artifact_metrics.part_count,
            "part_bytes": artifact_metrics.part_bytes,
            "oldest_part_age_seconds": artifact_metrics.oldest_part_age_seconds,
            "part_invalid_sidecar_count": artifact_metrics.part_invalid_sidecar_count,
            "part_invalid_sidecar_bytes": artifact_metrics.part_invalid_sidecar_bytes,
            "quarantine_count": artifact_metrics.quarantine_count,
            "quarantine_bytes": artifact_metrics.quarantine_bytes,
            "oldest_quarantine_age_seconds": artifact_metrics.oldest_quarantine_age_seconds,
            "quarantine_invalid_sidecar_count": artifact_metrics.quarantine_invalid_sidecar_count,
            "quarantine_invalid_sidecar_bytes": artifact_metrics.quarantine_invalid_sidecar_bytes,
            "artifact_sidecar_invalid": bool(
                artifact_metrics.part_invalid_sidecar_count
                or artifact_metrics.quarantine_invalid_sidecar_count
            ),
            "part_hard_threshold_crossed": bool(
                artifact_metrics.part_bytes >= storage.part_max_bytes
                or artifact_metrics.oldest_part_age_seconds
                >= storage.part_max_age_seconds
            ),
            "part_warning_threshold_crossed": bool(
                artifact_metrics.part_bytes >= storage.part_warning_bytes
                or (
                    artifact_metrics.oldest_part_age_seconds > 0
                    and artifact_metrics.oldest_part_age_seconds
                    >= storage.part_warning_age_seconds
                )
            ),
            "quarantine_warning_threshold_crossed": bool(
                artifact_metrics.quarantine_bytes >= storage.quarantine_warning_bytes
                or artifact_metrics.oldest_quarantine_age_seconds
                >= storage.quarantine_warning_age_seconds
            ),
            "quarantine_hard_threshold_crossed": bool(
                artifact_metrics.quarantine_bytes >= storage.quarantine_hard_bytes
                or artifact_metrics.oldest_quarantine_age_seconds
                >= storage.quarantine_hard_age_seconds
            ),
            "artifact_evidence": artifact_evidence,
        }


def _count_table(conn: Any, table: str) -> int:
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    except Exception as exc:
        if "no such table" in str(exc).lower():
            return 0
        raise
    return int(row[0])


def _canonical_consumer_name(value: str) -> str:
    """Normalize legacy rollout labels to the canonical consumer identities."""

    normalized = str(value or "").strip().lower().replace("-", "_")
    aliases = {
        "businessprofile": "business_profile",
        "broker": "broker_risk_control",
        "broker_risk": "broker_risk_control",
    }
    return aliases.get(normalized, normalized)


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
