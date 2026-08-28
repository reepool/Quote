"""Controlled review decisions for governed company business-profile records."""

from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional, Sequence

from research.business_profile_governance import (
    REVIEW_STATUSES,
    BusinessProfileRepository,
    _json_dumps,
    _json_loads,
    _stable_hash,
)
from research.business_profile_activity_production import (
    RELATIONSHIP_IDENTITY_DISCLOSED,
    canonical_relationship_identity_status,
)
from research.business_profile_temporal import (
    get_business_profile_supersession_column,
)
from utils.date_utils import get_shanghai_time


REVIEW_DECISIONS = {"held", "approved", "rejected", "superseded"}
SYSTEM_PROMOTION_SCHEMA_VERSION = "business_profile_system_promotion.v1"
SYSTEM_PROMOTION_REVIEWER_PREFIX = "system:business_profile_auto_promotion."
SYSTEM_REOPEN_SCHEMA_VERSION = "business_profile_system_reopen.v1"
OFFICIAL_EVIDENCE_SOURCE_TIERS = {
    "official_backup",
    "official_filing",
    "official_primary",
}


class BusinessProfileReviewService:
    """Apply optimistic review decisions and append immutable audit records."""

    def __init__(self, repository: BusinessProfileRepository):
        self.repository = repository
        self.storage = repository.storage

    def review_record(
        self,
        record_type: str,
        record_id: str,
        *,
        decision: str,
        reviewer: str,
        reason: str,
        expected_review_status: str,
        expected_updated_at: str,
        evidence_references: Optional[Sequence[str]] = None,
        replacement_record_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        _system_promotion: bool = False,
    ) -> Dict[str, Any]:
        """Apply one optimistic, audited review decision in a single transaction."""
        spec = self._record_spec(record_type)
        normalized_decision = str(decision or "").strip().lower()
        if normalized_decision not in REVIEW_DECISIONS:
            raise ValueError(f"unsupported review decision: {normalized_decision}")
        normalized_reviewer = str(reviewer or "").strip()
        normalized_reason = str(reason or "").strip()
        if not normalized_reviewer:
            raise ValueError("reviewer is required")
        if normalized_reviewer.startswith("system:") and not _system_promotion:
            raise ValueError("system reviewer identities require system_promote_record")
        if not normalized_reason:
            raise ValueError("reason is required")
        expected_status = str(expected_review_status or "").strip().lower()
        if expected_status not in REVIEW_STATUSES:
            raise ValueError(f"unsupported expected_review_status: {expected_status}")
        expected_updated = str(expected_updated_at or "").strip()
        if not expected_updated:
            raise ValueError("expected_updated_at is required")
        if _system_promotion:
            allowed_expected_statuses = {"candidate"}
        elif normalized_decision == "superseded":
            allowed_expected_statuses = {"approved"}
        elif normalized_decision == "held":
            allowed_expected_statuses = {"candidate"}
        else:
            allowed_expected_statuses = {"candidate", "held"}
        if expected_status not in allowed_expected_statuses:
            required_status = "/".join(sorted(allowed_expected_statuses))
            raise ValueError(
                f"{normalized_decision} requires expected_review_status={required_status}"
            )
        replacement_id = str(replacement_record_id or "").strip() or None
        if normalized_decision == "superseded" and not replacement_id:
            raise ValueError("replacement_record_id is required for superseded")
        references = sorted(
            {
                str(item).strip()
                for item in (evidence_references or ())
                if str(item).strip()
            }
        )
        now = get_shanghai_time().isoformat()
        operation_id = f"bp-review-op-{uuid.uuid4().hex}"

        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._load_review_row(conn, spec, record_id)
                if row is None:
                    raise ValueError(
                        f"business profile record not found: {record_type}:{record_id}"
                    )
                if _system_promotion:
                    self._validate_no_prior_human_decision(
                        conn,
                        record_type=record_type,
                        record_id=record_id,
                    )
                self._validate_expected_review_state(
                    row,
                    expected_status=expected_status,
                    expected_updated_at=expected_updated,
                )
                if normalized_decision == "approved":
                    if record_type == "relationships":
                        relationship_metadata = _json_loads(
                            row.get("metadata_json"), {}
                        )
                        has_identity_status = any(
                            str(relationship_metadata.get(key) or "").strip()
                            for key in ("identity_status", "resolution_status")
                        )
                        identity_status = canonical_relationship_identity_status(
                            relationship_metadata
                        )
                        if has_identity_status and identity_status is None:
                            raise ValueError(
                                "relationship identity statuses are conflicting or unknown"
                        )
                        if (
                            has_identity_status
                            and identity_status == RELATIONSHIP_IDENTITY_DISCLOSED
                            and not bool((metadata or {}).get("confirm_disclosed_name_only"))
                        ):
                            raise ValueError(
                                "disclosed-name-only relationship approval requires explicit confirmation"
                            )
                    self._validate_approval_evidence(
                        conn,
                        record_type,
                        row,
                        evidence_references=references,
                    )
                    self.repository._validate_temporal_state(
                        conn,
                        record_type,
                        {
                            "spec": spec,
                            "payload": row,
                            "pk_value": str(row[spec["pk"]]),
                            "status": "approved",
                        },
                    )
                if normalized_decision == "superseded":
                    replacement = self._load_review_row(conn, spec, replacement_id)
                    self._validate_replacement(
                        record_type,
                        row,
                        replacement,
                        replacement_id=replacement_id,
                    )

                audit = self._update_review_status(
                    conn,
                    spec=spec,
                    record_type=record_type,
                    row=row,
                    new_status=normalized_decision,
                    operation_id=operation_id,
                    reviewer=normalized_reviewer,
                    reason=normalized_reason,
                    evidence_references=references,
                    replacement_record_id=replacement_id,
                    metadata=metadata or {},
                    now=now,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return audit

    def system_promote_record(
        self,
        record_type: str,
        record_id: str,
        *,
        field_family: str,
        policy_version: str,
        gate_manifest_hash: str,
        reviewer_version: str,
        expected_updated_at: str,
        evidence_references: Optional[Sequence[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Approve one candidate through the normal optimistic audit transition."""

        normalized = {
            "field_family": str(field_family or "").strip(),
            "policy_version": str(policy_version or "").strip(),
            "gate_manifest_hash": str(gate_manifest_hash or "").strip(),
            "reviewer_version": str(reviewer_version or "").strip(),
        }
        missing = [key for key, value in normalized.items() if not value]
        if missing:
            raise ValueError(
                "system promotion identity is incomplete: " + ", ".join(sorted(missing))
            )
        if not normalized["reviewer_version"].startswith("v"):
            raise ValueError("reviewer_version must be versioned")
        promotion_metadata = dict(metadata or {})
        promotion_metadata["system_promotion"] = {
            "schema_version": SYSTEM_PROMOTION_SCHEMA_VERSION,
            **normalized,
        }
        reviewer = SYSTEM_PROMOTION_REVIEWER_PREFIX + normalized["reviewer_version"]
        return self.review_record(
            record_type,
            record_id,
            decision="approved",
            reviewer=reviewer,
            reason=(
                "all fail-closed automatic-promotion gates passed under "
                f"{normalized['policy_version']}"
            ),
            expected_review_status="candidate",
            expected_updated_at=expected_updated_at,
            evidence_references=evidence_references,
            metadata=promotion_metadata,
            _system_promotion=True,
        )

    def system_reopen_rejected_record(
        self,
        record_type: str,
        record_id: str,
        *,
        expected_updated_at: str,
        reason: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Reopen an obsolete machine rejection while preserving audit history."""

        spec = self._record_spec(record_type)
        now = get_shanghai_time().isoformat()
        operation_id = f"bp-review-op-{uuid.uuid4().hex}"
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._load_review_row(conn, spec, record_id)
                if row is None:
                    raise ValueError(
                        f"business profile record not found: {record_type}:{record_id}"
                    )
                self._validate_no_prior_human_decision(
                    conn, record_type=record_type, record_id=record_id
                )
                self._validate_expected_review_state(
                    row,
                    expected_status="rejected",
                    expected_updated_at=str(expected_updated_at or "").strip(),
                )
                audit = self._update_review_status(
                    conn,
                    spec=spec,
                    record_type=record_type,
                    row=row,
                    new_status="candidate",
                    operation_id=operation_id,
                    reviewer=SYSTEM_PROMOTION_REVIEWER_PREFIX + "reopen.v1",
                    reason=str(
                        reason or "current automatic verification supersedes rejection"
                    ),
                    evidence_references=(),
                    replacement_record_id=None,
                    metadata={
                        **dict(metadata or {}),
                        "system_reopen": {
                            "schema_version": SYSTEM_REOPEN_SCHEMA_VERSION,
                            "prior_status": "rejected",
                        },
                    },
                    now=now,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return audit

    def system_hold_approved_record(
        self,
        record_type: str,
        record_id: str,
        *,
        expected_updated_at: str,
        reason: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Remove an unsafe machine-approved record from current reads.

        This narrowly scoped transition is for deterministic integrity repairs.
        It never overrides a human decision and retains the original row and
        its evidence as immutable review history.
        """

        spec = self._record_spec(record_type)
        now = get_shanghai_time().isoformat()
        operation_id = f"bp-review-op-{uuid.uuid4().hex}"
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._load_review_row(conn, spec, record_id)
                if row is None:
                    raise ValueError(
                        f"business profile record not found: {record_type}:{record_id}"
                    )
                self._validate_no_prior_human_decision(
                    conn, record_type=record_type, record_id=record_id
                )
                self._validate_expected_review_state(
                    row,
                    expected_status="approved",
                    expected_updated_at=str(expected_updated_at or "").strip(),
                )
                audit = self._update_review_status(
                    conn,
                    spec=spec,
                    record_type=record_type,
                    row=row,
                    new_status="held",
                    operation_id=operation_id,
                    reviewer=SYSTEM_PROMOTION_REVIEWER_PREFIX + "integrity_hold.v1",
                    reason=str(reason or "deterministic integrity repair requires review"),
                    evidence_references=(),
                    replacement_record_id=None,
                    metadata={
                        **dict(metadata or {}),
                        "system_integrity_hold": {
                            "schema_version": "business_profile_system_integrity_hold.v1",
                            "prior_status": "approved",
                        },
                    },
                    now=now,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return audit

    def system_hold_candidate(
        self,
        record_type: str,
        record_id: str,
        *,
        expected_updated_at: str,
        reason: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Hold a machine candidate through the audited review owner."""

        return self.review_record(
            record_type,
            record_id,
            decision="held",
            reviewer=SYSTEM_PROMOTION_REVIEWER_PREFIX + "integrity_hold.v1",
            reason=reason,
            expected_review_status="candidate",
            expected_updated_at=expected_updated_at,
            metadata={
                **dict(metadata or {}),
                "system_integrity_hold": {
                    "schema_version": "business_profile_system_integrity_hold.v1",
                    "prior_status": "candidate",
                },
            },
            _system_promotion=True,
        )

    def list_review_audit(
        self,
        *,
        record_type: Optional[str] = None,
        record_id: Optional[str] = None,
        instrument_id: Optional[str] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """Read append-only review decisions in reverse chronological order."""
        if record_type:
            self._record_spec(record_type)
        clauses: List[str] = []
        params: List[Any] = []
        for column, value in (
            ("record_type", record_type),
            ("record_id", record_id),
            ("instrument_id", instrument_id),
        ):
            normalized = str(value or "").strip()
            if normalized:
                clauses.append(f"{column} = ?")
                params.append(normalized)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(1, min(int(limit), 10000)))
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                f"""
                SELECT *
                FROM business_profile_review_audit
                {where}
                ORDER BY reviewed_at DESC, audit_id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        output: List[Dict[str, Any]] = []
        for item in rows:
            decoded = dict(item)
            decoded["evidence_references"] = _json_loads(
                decoded.pop("evidence_references_json", None),
                [],
            )
            decoded["metadata"] = _json_loads(
                decoded.pop("metadata_json", None),
                {},
            )
            output.append(decoded)
        return output

    @staticmethod
    def _record_spec(record_type: str) -> Dict[str, Any]:
        normalized = str(record_type or "").strip()
        spec = BusinessProfileRepository._TABLES.get(normalized)
        if spec is None:
            raise ValueError(f"unsupported business profile record type: {normalized}")
        return spec

    @staticmethod
    def _load_review_row(
        conn: Any,
        spec: Dict[str, Any],
        record_id: str,
    ) -> Optional[Dict[str, Any]]:
        row = conn.execute(
            f"SELECT * FROM {spec['table']} WHERE {spec['pk']} = ?",
            (record_id,),
        ).fetchone()
        return dict(row) if row is not None else None

    @staticmethod
    def _validate_expected_review_state(
        row: Dict[str, Any],
        *,
        expected_status: str,
        expected_updated_at: str,
    ) -> None:
        actual_status = str(row.get("review_status") or "")
        actual_updated_at = str(row.get("updated_at") or "")
        if actual_status != expected_status or actual_updated_at != expected_updated_at:
            raise ValueError(
                "stale business profile review state: "
                f"expected {expected_status}@{expected_updated_at}, "
                f"found {actual_status}@{actual_updated_at}"
            )

    @staticmethod
    def _validate_no_prior_human_decision(
        conn: Any,
        *,
        record_type: str,
        record_id: str,
    ) -> None:
        row = conn.execute(
            """
            SELECT reviewer, decision
            FROM business_profile_review_audit
            WHERE record_type = ? AND record_id = ?
              AND reviewer NOT LIKE 'system:%'
              AND reviewer NOT LIKE 'automation:%'
            ORDER BY reviewed_at DESC, audit_id DESC
            LIMIT 1
            """,
            (record_type, record_id),
        ).fetchone()
        if row is not None:
            raise ValueError(
                "prior human decision blocks automatic promotion: "
                f"{record_type}:{record_id}:{row['decision']}"
            )

    @staticmethod
    def _validate_approval_evidence(
        conn: Any,
        record_type: str,
        row: Dict[str, Any],
        *,
        evidence_references: Sequence[str],
    ) -> None:
        if record_type == "evidence":
            source_tier = str(row.get("source_tier") or "").strip().lower()
            if (
                source_tier not in OFFICIAL_EVIDENCE_SOURCE_TIERS
                and not evidence_references
            ):
                raise ValueError(
                    "non-official evidence approval requires evidence_references"
                )
            return
        evidence_id = str(row.get("evidence_id") or "").strip()
        if record_type == "exposure_assumptions" and not evidence_id:
            if not evidence_references:
                raise ValueError(
                    "calibrated exposure assumption approval requires evidence_references"
                )
            return
        evidence = conn.execute(
            """
            SELECT instrument_id, review_status
            FROM business_profile_evidence
            WHERE evidence_id = ?
            """,
            (evidence_id,),
        ).fetchone()
        if evidence is None:
            raise ValueError(f"approval evidence not found: {evidence_id}")
        if evidence["review_status"] != "approved":
            raise ValueError(f"approval evidence is not approved: {evidence_id}")
        if evidence["instrument_id"] != row.get("instrument_id"):
            raise ValueError(f"approval evidence instrument mismatch: {evidence_id}")

    @staticmethod
    def _validate_replacement(
        record_type: str,
        row: Dict[str, Any],
        replacement: Optional[Dict[str, Any]],
        *,
        replacement_id: str,
    ) -> None:
        if replacement is None:
            raise ValueError(
                f"replacement record not found: {record_type}:{replacement_id}"
            )
        current_id = (
            row.get("activity_id")
            or row.get("relationship_id")
            or row.get("record_id")
            or row.get("exposure_id")
            or row.get("fact_id")
            or row.get("assumption_id")
            or row.get("event_id")
            or row.get("regime_id")
            or row.get("evidence_id")
        )
        if replacement_id == current_id:
            raise ValueError("replacement record must differ from reviewed record")
        if replacement.get("instrument_id") != row.get("instrument_id"):
            raise ValueError("replacement record instrument mismatch")
        if replacement.get("review_status") != "approved":
            raise ValueError("replacement record must already be approved")
        if not BusinessProfileRepository._same_temporal_identity(
            record_type, row, replacement
        ):
            raise ValueError("replacement record stable identity mismatch")
        pointer = replacement.get(get_business_profile_supersession_column(record_type))
        if str(pointer or "") != str(current_id):
            raise ValueError("replacement record supersession pointer mismatch")

    def _update_review_status(
        self,
        conn: Any,
        *,
        spec: Dict[str, Any],
        record_type: str,
        row: Dict[str, Any],
        new_status: str,
        operation_id: str,
        reviewer: str,
        reason: str,
        evidence_references: Sequence[str],
        replacement_record_id: Optional[str],
        metadata: Dict[str, Any],
        now: str,
    ) -> Dict[str, Any]:
        assignments = ["review_status = ?", "updated_at = ?"]
        values: List[Any] = [new_status, now]
        if record_type == "evidence":
            assignments.extend(["reviewed_by = ?", "reviewed_at = ?"])
            values.extend([reviewer, now])
        values.extend(
            [
                row[spec["pk"]],
                row["review_status"],
                row["updated_at"],
            ]
        )
        result = conn.execute(
            f"""
            UPDATE {spec['table']}
            SET {', '.join(assignments)}
            WHERE {spec['pk']} = ?
              AND review_status = ?
              AND updated_at = ?
            """,
            values,
        )
        if result.rowcount != 1:
            raise ValueError("stale business profile review state")

        audit = self._build_review_audit(
            conn,
            record_type=record_type,
            record_id=str(row[spec["pk"]]),
            row=row,
            new_status=new_status,
            operation_id=operation_id,
            reviewer=reviewer,
            reason=reason,
            evidence_references=evidence_references,
            replacement_record_id=replacement_record_id,
            metadata=metadata,
            now=now,
        )
        conn.execute(
            """
            INSERT INTO business_profile_review_audit (
                audit_id, operation_id, record_type, record_id, instrument_id,
                decision, prior_status, new_status, prior_version, new_version,
                prior_updated_at, new_updated_at, record_lineage_hash, reviewer,
                reason, evidence_references_json, replacement_record_id,
                prior_audit_hash, audit_hash, metadata_json, reviewed_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                audit["audit_id"],
                audit["operation_id"],
                audit["record_type"],
                audit["record_id"],
                audit["instrument_id"],
                audit["decision"],
                audit["prior_status"],
                audit["new_status"],
                audit["prior_version"],
                audit["new_version"],
                audit["prior_updated_at"],
                audit["new_updated_at"],
                audit["record_lineage_hash"],
                audit["reviewer"],
                audit["reason"],
                _json_dumps(audit["evidence_references"]),
                audit["replacement_record_id"],
                audit["prior_audit_hash"],
                audit["audit_hash"],
                _json_dumps(audit["metadata"]),
                audit["reviewed_at"],
                audit["created_at"],
            ),
        )
        return audit

    @staticmethod
    def _build_review_audit(
        conn: Any,
        *,
        record_type: str,
        record_id: str,
        row: Dict[str, Any],
        new_status: str,
        operation_id: str,
        reviewer: str,
        reason: str,
        evidence_references: Sequence[str],
        replacement_record_id: Optional[str],
        metadata: Dict[str, Any],
        now: str,
    ) -> Dict[str, Any]:
        prior = conn.execute(
            """
            SELECT audit_hash
            FROM business_profile_review_audit
            WHERE record_type = ? AND record_id = ?
            ORDER BY reviewed_at DESC, audit_id DESC
            LIMIT 1
            """,
            (record_type, record_id),
        ).fetchone()
        prior_audit_hash = str(prior["audit_hash"]) if prior is not None else None
        payload = {
            "operation_id": operation_id,
            "record_type": record_type,
            "record_id": record_id,
            "instrument_id": row["instrument_id"],
            "decision": new_status,
            "prior_status": row["review_status"],
            "new_status": new_status,
            "prior_version": row.get("version"),
            "new_version": row.get("version"),
            "prior_updated_at": row["updated_at"],
            "new_updated_at": now,
            "record_lineage_hash": row.get("lineage_hash"),
            "reviewer": reviewer,
            "reason": reason,
            "evidence_references": list(evidence_references),
            "replacement_record_id": replacement_record_id,
            "prior_audit_hash": prior_audit_hash,
            "metadata": metadata,
            "reviewed_at": now,
            "created_at": now,
        }
        payload["audit_hash"] = _stable_hash(payload)
        payload["audit_id"] = f"bp-review-{uuid.uuid4().hex}"
        return payload
