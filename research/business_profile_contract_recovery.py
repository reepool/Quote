"""Idempotent recovery of shadow candidates created under obsolete contracts."""

from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from research.business_profile_review import BusinessProfileReviewService
from research.business_profile_semantic_artifacts import (
    BusinessProfileSemanticArtifactRepository,
    SemanticArtifactIdentity,
)
from research.business_profile_semantic_extraction import (
    STRUCTURED_EXTRACTION_PROMPT_VERSION,
    STRUCTURED_EXTRACTION_SCHEMA_VERSION,
)
from research.business_profile_semantic_runtime import RUNTIME_SCHEMA_VERSION
from utils.date_utils import get_shanghai_time


CONTRACT_AUDIT_VERSION = "business_profile_contract_recovery.v1"


class BusinessProfileContractRecovery:
    def __init__(self, repository: Any) -> None:
        self.repository = repository
        self.storage = repository.storage
        self.review = BusinessProfileReviewService(repository)

    def run(self, *, limit: int = 5000) -> dict[str, Any]:
        rejected = reopened = approved_blockers = requeued = scanned = 0
        human_held = 0
        affected: list[dict[str, str]] = []
        for record_type in ("segments", "operating_facts"):
            rows = [
                *self._obsolete_rows(
                    record_type,
                    review_statuses=("candidate", "held"),
                    limit=limit,
                ),
                *self._obsolete_rows(
                    record_type,
                    review_statuses=("approved",),
                    limit=limit,
                    exclude_existing_blockers=True,
                ),
            ]
            for row in rows:
                scanned += 1
                reasons = obsolete_contract_reasons(record_type, row)
                if not reasons:
                    continue
                record_id = str(
                    row[self.repository._TABLES[record_type]["pk"]]
                )
                status = str(row.get("review_status") or "")
                if status == "approved":
                    self._upsert_blocker(record_type, record_id, row, reasons)
                    approved_blockers += 1
                    continue
                if status == "held" and not self._automation_owned_hold(row):
                    # A hold is an explicit review decision.  Contract recovery
                    # may only revisit a hold that carries automation provenance.
                    human_held += 1
                    affected.append(
                        {
                            "record_type": record_type,
                            "record_id": record_id,
                            "instrument_id": str(row.get("instrument_id") or ""),
                            "status": "human_held",
                        }
                    )
                    continue
                if status not in {"candidate", "held"}:
                    continue
                self.review.review_record(
                    record_type,
                    record_id,
                    decision="rejected",
                    reviewer=f"automation:{CONTRACT_AUDIT_VERSION}",
                    reason="obsolete production contract: " + ",".join(reasons),
                    expected_review_status=status,
                    expected_updated_at=str(row["updated_at"]),
                    evidence_references=[str(row.get("evidence_id") or "")],
                    metadata={
                        "contract_audit_version": CONTRACT_AUDIT_VERSION,
                        "reason_codes": reasons,
                    },
                )
                rejected += 1
                requeued += self._requeue_semantic(row, reasons)
                affected.append(
                    {
                        "record_type": record_type,
                        "record_id": record_id,
                        "instrument_id": str(row.get("instrument_id") or ""),
                    }
                )
            # A previous version of this audit incorrectly rejected semantic
            # relationship/concentration facts as obsolete structured rows.
            # Reopen only that automation-owned rejection; human decisions
            # remain protected by system_reopen_rejected_record().
            for row in self._recoverable_rejected_rows(record_type, limit=limit):
                scanned += 1
                if obsolete_contract_reasons(record_type, row):
                    continue
                record_id = str(row[self.repository._TABLES[record_type]["pk"]])
                try:
                    self.review.system_reopen_rejected_record(
                        record_type,
                        record_id,
                        expected_updated_at=str(row["updated_at"]),
                        reason=(
                            "current contract no longer classifies this record as "
                            "obsolete"
                        ),
                        metadata={"contract_audit_version": CONTRACT_AUDIT_VERSION},
                    )
                except ValueError:
                    # Prior human decisions and concurrent state changes are
                    # intentionally left untouched.
                    continue
                reopened += 1
                requeued += self._requeue_semantic(
                    row, ("contract_recovery_reopened",)
                )
                affected.append(
                    {
                        "record_type": record_type,
                        "record_id": record_id,
                        "instrument_id": str(row.get("instrument_id") or ""),
                    }
                )
        unit_recovery = self.recover_unit_blocked(limit=20)
        return {
            "schema_version": CONTRACT_AUDIT_VERSION,
            "scanned": scanned,
            "rejected": rejected,
            "reopened": reopened,
            "approved_history_blockers": approved_blockers,
            "human_held": human_held,
            "requeued": requeued,
            "affected": affected[:100],
            "unit_blocked_recovery": unit_recovery,
        }

    @staticmethod
    def _automation_owned_hold(row: Mapping[str, Any]) -> bool:
        """Return true only for a hold carrying explicit automation provenance."""

        metadata = row.get("metadata")
        if not isinstance(metadata, Mapping):
            return False
        provenance = str(
            metadata.get("hold_provenance")
            or metadata.get("review_provenance")
            or ""
        ).strip()
        return provenance.startswith("automation:")

    def _obsolete_rows(
        self,
        record_type: str,
        *,
        review_statuses: Sequence[str],
        limit: int,
        exclude_existing_blockers: bool = False,
    ) -> list[dict[str, Any]]:
        """Select only rows that can fail the current contract audit."""

        spec = self.repository._TABLES[record_type]
        placeholders = ",".join("?" for _ in review_statuses)
        metadata = "metadata_json"
        clauses = [f"review_status IN ({placeholders})"]
        params: list[Any] = list(review_statuses)
        clauses.append(
            "("
            f"json_extract({metadata}, '$.numeric_reconciliation_executed') IS NOT 1 "
            f"OR json_extract({metadata}, '$.numeric_reconciliation_valid') IS NOT 1 "
            f"OR (json_extract({metadata}, '$.semantic_synthesis') IS 1 AND ("
            f"COALESCE(json_extract({metadata}, '$.structured_schema_version'), '') <> ? "
            f"OR TRIM(COALESCE(json_extract({metadata}, '$.source_label_raw'), '')) = '' "
            f"OR (TRIM(COALESCE(json_extract({metadata}, '$.semantic_summary_zh'), '')) <> '' "
            f"AND NOT CAST(json_extract({metadata}, '$.semantic_summary_zh') AS TEXT) "
            "GLOB ('*[' || char(13312) || '-' || char(40959) || ']*')))) "
            f"OR (TRIM(COALESCE(json_extract({metadata}, '$.runtime_schema_version'), '')) <> '' "
            f"AND json_extract({metadata}, '$.runtime_schema_version') <> ?)"
            ")"
        )
        params.extend([STRUCTURED_EXTRACTION_SCHEMA_VERSION, RUNTIME_SCHEMA_VERSION])
        if exclude_existing_blockers:
            clauses.append(
                "NOT EXISTS (SELECT 1 FROM business_profile_readiness_blockers b "
                f"WHERE b.blocker_type = 'approved_history_conflict' "
                f"AND b.target_type = ? AND b.target_id = {spec['table']}.{spec['pk']} "
                "AND b.status = 'open')"
            )
            params.append(record_type)
        params.append(max(1, min(int(limit), 10000)))
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                f"SELECT * FROM {spec['table']} WHERE {' AND '.join(clauses)} "
                f"ORDER BY updated_at, {spec['pk']} LIMIT ?",
                params,
            ).fetchall()
        return [
            self.repository._decode_row(dict(row), spec["json"]) for row in rows
        ]

    def _recoverable_rejected_rows(
        self, record_type: str, *, limit: int
    ) -> list[dict[str, Any]]:
        """Find records rejected by the old automated contract audit."""

        spec = self.repository._TABLES[record_type]
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                f"SELECT r.* FROM {spec['table']} r WHERE r.review_status = 'rejected' "
                "AND EXISTS (SELECT 1 FROM business_profile_review_audit a "
                "WHERE a.record_type = ? AND a.record_id = r." + spec["pk"] + " "
                "AND a.reviewer = ? AND a.decision = 'rejected') "
                f"ORDER BY r.updated_at, r.{spec['pk']} LIMIT ?",
                (record_type, f"automation:{CONTRACT_AUDIT_VERSION}", max(1, min(int(limit), 10000))),
            ).fetchall()
        return [
            self.repository._decode_row(dict(row), spec["json"]) for row in rows
        ]

    def recover_unit_blocked(self, *, limit: int = 20) -> dict[str, Any]:
        """Migrate complete audit responses into v4 artifacts without model calls."""

        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                "SELECT * FROM business_profile_exceptions "
                "WHERE status = 'open' AND EXISTS ("
                "SELECT 1 FROM json_each(reason_codes_json) "
                "WHERE json_each.value = ?) "
                "ORDER BY created_at LIMIT ?",
                ("unit_normalization_failed", max(1, int(limit))),
            ).fetchall()
        artifacts = BusinessProfileSemanticArtifactRepository(self.storage)
        recovered = 0
        skipped = 0
        for raw in rows:
            exception = dict(raw)
            metadata = _json_object(exception.get("metadata_json"))
            diagnostics = dict(metadata.get("diagnostics") or {})
            audit = dict(diagnostics.get("semantic_audit") or {})
            model_payload = dict(
                ((audit.get("diagnostics") or {}).get("semantic_result") or {})
            )
            selected_path = str(metadata.get("selected_artifact_path") or "")
            if not model_payload or not selected_path:
                skipped += 1
                continue
            try:
                selected = json.loads(
                    gzip.decompress(Path(selected_path).read_bytes()).decode("utf-8")
                )
                work = self._work_item(str(exception.get("target_id") or ""))
                report_period = str(work.get("report_period") or "")
                bundle = dict(selected["bundle"])
                sections = list(selected.get("sections") or [])
                evidence_scope = [
                    {
                        "section_id": str(section["section_id"]),
                        "page_number": int(section["page_number"]),
                        "section_hash": str(section["section_hash"]),
                    }
                    for section in sections
                ]
                rows_payload = list(model_payload.get("rows") or [])
                resolved = list(
                    ((audit.get("diagnostics") or {}).get("resolved_evidence") or [])
                )
                for index, row_payload in enumerate(rows_payload):
                    if "evidence" not in row_payload and index < len(resolved):
                        row_payload["evidence"] = resolved[index]
                model_payload.update(
                    {
                        "schema_version": STRUCTURED_EXTRACTION_SCHEMA_VERSION,
                        "field_family": str(exception.get("field_family") or ""),
                        "instrument_id": str(exception.get("instrument_id") or ""),
                        "report_period": report_period,
                        "rows": rows_payload,
                    }
                )
                input_scope = {
                    "bundle_id": bundle.get("bundle_id"),
                    "field_family": exception.get("field_family"),
                    "instrument_id": exception.get("instrument_id"),
                    "report_period": report_period,
                    "evidence_scope": evidence_scope,
                    "schema_version": STRUCTURED_EXTRACTION_SCHEMA_VERSION,
                }
                identity = SemanticArtifactIdentity(
                    instrument_id=str(exception["instrument_id"]),
                    source_document_id=str(metadata["source_document_id"]),
                    document_hash=str(bundle["document_hash"]),
                    report_period=report_period,
                    field_family=str(exception["field_family"]),
                    evidence_scope_hash=_stable_hash(evidence_scope),
                    input_hash=_stable_hash(input_scope),
                    prompt_version=STRUCTURED_EXTRACTION_PROMPT_VERSION,
                    schema_version=STRUCTURED_EXTRACTION_SCHEMA_VERSION,
                )
                receipt = artifacts.receive(
                    identity,
                    response=model_payload,
                    response_hash=str(audit.get("response_hash") or ""),
                    evidence_ids=[
                        str(span.get("evidence_span_id"))
                        for row_payload in rows_payload
                        for span in (
                            (row_payload.get("evidence") or {}).get("evidence_spans")
                            or []
                        )
                        if span.get("evidence_span_id")
                    ],
                    model_profile=str(audit.get("profile") or "") or None,
                    actual_model=str(audit.get("actual_model") or "") or None,
                    usage=dict(audit.get("usage") or {}),
                    authority={
                        "recovered_from_audit": True,
                        "model_derived_hints": "diagnostic_only",
                    },
                )
                artifacts.mark(
                    str(receipt["artifact_id"]),
                    "conversion_pending",
                    reason_code="recovered_unit_blocked_audit",
                    runtime_version=RUNTIME_SCHEMA_VERSION,
                )
                # Requeue first: both operations are idempotent, and leaving the
                # exception open makes a crash between them safely resumable.
                self._requeue_recovered_work(work)
                self._resolve_exception(
                    str(exception["exception_id"]),
                    {
                        "recovered_artifact_id": receipt["artifact_id"],
                        "recovery_version": CONTRACT_AUDIT_VERSION,
                    },
                )
                recovered += 1
            except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
                skipped += 1
        return {"attempted": len(rows), "recovered": recovered, "skipped": skipped}

    def _work_item(self, work_id: str) -> dict[str, Any]:
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            row = conn.execute(
                "SELECT * FROM business_profile_work_items WHERE work_id = ?",
                (work_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown work item: {work_id}")
        return dict(row)

    def _resolve_exception(self, exception_id: str, metadata: Mapping[str, Any]) -> None:
        now = get_shanghai_time().isoformat()
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            row = conn.execute(
                "SELECT metadata_json FROM business_profile_exceptions "
                "WHERE exception_id = ? AND status = 'open'",
                (exception_id,),
            ).fetchone()
            if row is None:
                return
            existing = _json_object(row["metadata_json"])
            existing["contract_recovery"] = dict(metadata)
            conn.execute(
                "UPDATE business_profile_exceptions SET status = 'resolved', "
                "resolved_at = ?, metadata_json = ?, updated_at = ? "
                "WHERE exception_id = ? AND status = 'open'",
                (
                    now,
                    json.dumps(existing, ensure_ascii=False, sort_keys=True),
                    now,
                    exception_id,
                ),
            )
            conn.commit()

    def _requeue_recovered_work(self, work: Mapping[str, Any]) -> None:
        now = get_shanghai_time().isoformat()
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute(
                "UPDATE business_profile_work_items SET stage = 'semantic', status = 'retry_due', "
                "next_attempt_at = NULL, lease_owner = NULL, lease_expires_at = NULL, "
                "last_error = 'recovered_unit_blocked_audit', updated_at = ? "
                "WHERE work_id = ?",
                (now, work.get("work_id")),
            )
            conn.commit()

    def _requeue_semantic(
        self, row: Mapping[str, Any], reasons: Sequence[str]
    ) -> int:
        now = get_shanghai_time().isoformat()
        source_document_id = str(row.get("source_document_id") or "")
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            cursor = conn.execute(
                """
                UPDATE business_profile_work_items
                SET stage = 'semantic', status = 'retry_due', lease_owner = NULL,
                    lease_expires_at = NULL, next_attempt_at = NULL,
                    last_error = ?, updated_at = ?
                WHERE instrument_id = ?
                  AND status NOT IN ('superseded', 'terminal_failure')
                  AND (metadata_json LIKE ? OR ? = '')
                """,
                (
                    "contract_recovery:" + ",".join(reasons),
                    now,
                    row.get("instrument_id"),
                    f"%{source_document_id}%",
                    source_document_id,
                ),
            )
            conn.commit()
        return int(cursor.rowcount or 0)

    def _upsert_blocker(
        self,
        record_type: str,
        record_id: str,
        row: Mapping[str, Any],
        reasons: Sequence[str],
    ) -> None:
        now = get_shanghai_time().isoformat()
        blocker_id = "bp-readiness-blocker-" + _stable_hash(
            {"type": record_type, "id": record_id, "reasons": sorted(reasons)}
        )[:24]
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute(
                """
                INSERT OR IGNORE INTO business_profile_readiness_blockers (
                    blocker_id, blocker_type, instrument_id, target_type,
                    target_id, status, details_json, created_at, updated_at
                ) VALUES (?, 'approved_history_conflict', ?, ?, ?, 'open', ?, ?, ?)
                """,
                (
                    blocker_id,
                    row.get("instrument_id"),
                    record_type,
                    record_id,
                    json.dumps(
                        {"reason_codes": list(reasons)},
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    now,
                    now,
                ),
            )
            conn.commit()


def obsolete_contract_reasons(
    record_type: str, row: Mapping[str, Any]
) -> tuple[str, ...]:
    metadata = dict(row.get("metadata") or {})
    reasons: list[str] = []
    if metadata.get("numeric_reconciliation_executed") is not True:
        reasons.append("numeric_reconciliation_not_executed")
    if metadata.get("numeric_reconciliation_valid") is not True:
        reasons.append("numeric_reconciliation_invalid")
    # Relationship/concentration candidates are semantic synthesis records,
    # but they are not structured-table extraction records.  They have no
    # structured schema/source-label contract and must not be rejected for
    # lacking those fields.
    structured_contract = metadata.get("semantic_synthesis") is True and (
        record_type != "operating_facts"
        or metadata.get("structured_schema_version") is not None
    )
    if structured_contract:
        if (
            metadata.get("structured_schema_version")
            != STRUCTURED_EXTRACTION_SCHEMA_VERSION
        ):
            reasons.append("structured_schema_obsolete")
        if not str(metadata.get("source_label_raw") or "").strip():
            reasons.append("source_label_contract_obsolete")
        summary = str(metadata.get("semantic_summary_zh") or "").strip()
        if summary and not any("\u3400" <= char <= "\u9fff" for char in summary):
            reasons.append("language_contract_invalid")
    if str(metadata.get("runtime_schema_version") or RUNTIME_SCHEMA_VERSION) not in {
        RUNTIME_SCHEMA_VERSION,
        "",
    }:
        reasons.append("runtime_schema_obsolete")
    return tuple(dict.fromkeys(reasons))


def _stable_hash(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        decoded = json.loads(str(value or "{}"))
    except (TypeError, json.JSONDecodeError):
        return {}
    return dict(decoded) if isinstance(decoded, Mapping) else {}
