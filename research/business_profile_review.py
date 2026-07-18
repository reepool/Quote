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
from utils.date_utils import get_shanghai_time


REVIEW_DECISIONS = {"approved", "rejected", "superseded"}
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
        if not normalized_reason:
            raise ValueError("reason is required")
        expected_status = str(expected_review_status or "").strip().lower()
        if expected_status not in REVIEW_STATUSES:
            raise ValueError(f"unsupported expected_review_status: {expected_status}")
        expected_updated = str(expected_updated_at or "").strip()
        if not expected_updated:
            raise ValueError("expected_updated_at is required")
        required_status = (
            "approved" if normalized_decision == "superseded" else "candidate"
        )
        if expected_status != required_status:
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
                self._validate_expected_review_state(
                    row,
                    expected_status=expected_status,
                    expected_updated_at=expected_updated,
                )
                if normalized_decision == "approved":
                    self._validate_approval_evidence(
                        conn,
                        record_type,
                        row,
                        evidence_references=references,
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
            row.get("record_id")
            or row.get("exposure_id")
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
        pointer = (
            replacement.get("supersedes_exposure_id")
            if record_type == "exposures"
            else replacement.get("supersedes_record_id")
        )
        if pointer and str(pointer) != str(current_id):
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
