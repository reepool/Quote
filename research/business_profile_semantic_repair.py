"""Bounded local audit and repair for corrected business-profile semantics.

This service deliberately has no provider, announcement, PDF, or LLM dependency.
It reports derived-state problems from persisted local records and applies only
repairs that can be reconstructed without weakening evidence or review history.
"""

from __future__ import annotations

from collections import Counter
import hashlib
from typing import Any, Iterable

from research.business_profile_governance import BusinessProfileRepository
from research.providers.base import ShareholderSnapshot
from research.shareholder_snapshot_policy import (
    actual_shareholder_coverage_scope,
    normalize_shareholder_report_date,
)
from research.shareholder_sync import ShareholderShadowSyncService


REPAIR_SCHEMA_VERSION = "business_profile_semantic_repair.v1"


class BusinessProfileSemanticRepairService:
    """Audit and apply locally reconstructable semantic corrections only."""

    def __init__(self, storage: Any) -> None:
        self.storage = storage
        self.repository = BusinessProfileRepository(storage)

    def run(
        self,
        *,
        instrument_ids: Iterable[str] | None = None,
        apply: bool = False,
        all_scope: bool = False,
    ) -> dict[str, Any]:
        ids = sorted({str(item).strip() for item in instrument_ids or () if str(item).strip()})
        if apply and not ids and not all_scope:
            raise ValueError("repair apply requires instrument_ids or all_scope=True")
        if all_scope and ids:
            raise ValueError("repair accepts instrument_ids or all_scope, not both")
        if all_scope:
            ids = self._all_local_instrument_ids()
        if not ids:
            return self._report([], apply=apply)

        findings = [self._audit_instrument(instrument_id) for instrument_id in ids]
        result = self._report(findings, apply=apply)
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
        result["changes"] = changes
        result["change_counts"] = dict(Counter(item["status"] for item in changes))
        result["write_count"] = sum(item["status"] == "changed" for item in changes)
        return result

    def _all_local_instrument_ids(self) -> list[str]:
        tables = (
            "shareholder_snapshots",
            "company_business_profile_relationships",
            "company_commodity_exposures",
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

    def _audit_instrument(self, instrument_id: str) -> dict[str, Any]:
        issues: list[dict[str, Any]] = []
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
            name = str(snapshot.get("control_owner_name") or "").strip()
            if name:
                controller_names = {
                    str(item.get("actual_controller_name") or "").strip()
                    for item in self.storage.list_shareholder_control_changes(instrument_id)
                }
                if name not in controller_names:
                    issues.append(self._issue("shareholder_inferred_controller", instrument_id, {
                        "control_owner_name": name,
                    }))

        relationships = self.repository.list_records("relationships", instrument_id=instrument_id, limit=10000)
        for record in relationships:
            metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
            if (
                record.get("review_status") == "approved"
                and str(metadata.get("resolution_basis") or "") == "exact_legal_name"
                and self._is_short_name_resolution(record)
            ):
                issues.append(self._issue("relationship_short_name_auto_resolution", instrument_id, {
                    "relationship_id": record.get("relationship_id"),
                    "counterparty_name_raw": record.get("counterparty_name_raw"),
                }))
        issues.extend(self._relationship_lineage_findings(instrument_id, relationships))
        issues.extend(self._exposure_collision_findings(instrument_id))
        return {"instrument_id": instrument_id, "issues": issues}

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
            if issue["code"] in {"relationship_short_name_auto_resolution", "relationship_multiple_report_cohorts", "exposure_action_collision"}:
                changes.append({
                    "instrument_id": instrument_id,
                    "status": "held",
                    "reason": issue["code"],
                    "stable_id": issue["stable_id"],
                })
        return changes

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
                snapshot.get("control_owner_name")
                if scope == "reference_only_ownership_clues" else None
            ),
            control_owner_ratio=(
                snapshot.get("control_owner_ratio")
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
        return bool(profile and raw_name == str(profile.get("short_name") or "").strip()
                         and raw_name != str(profile.get("company_name") or "").strip())

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
        rows = self.repository.list_records("exposures", instrument_id=instrument_id, review_status="approved", limit=10000)
        groups: dict[tuple[Any, ...], set[str]] = {}
        for item in rows:
            key = (item.get("scope_type"), item.get("scope_id"), item.get("commodity_id"), item.get("exposure_role"))
            groups.setdefault(key, set()).add(str((item.get("metadata") or {}).get("source_activity_action") or ""))
        return [
            self._issue("exposure_action_collision", instrument_id, {"actions": sorted(actions)})
            for actions in groups.values() if len(actions - {""}) > 1
        ]

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
    def _report(findings: list[dict[str, Any]], *, apply: bool) -> dict[str, Any]:
        issues = [item for finding in findings for item in finding["issues"]]
        return {
            "schema_version": REPAIR_SCHEMA_VERSION,
            "mode": "apply" if apply else "audit",
            "network_access": False,
            "llm_access": False,
            "write_count": 0,
            "instruments": findings,
            "issue_counts": dict(Counter(item["code"] for item in issues)),
        }
