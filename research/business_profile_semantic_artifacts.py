"""Immutable semantic-response receipts and deterministic conversion replay."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence

from utils.date_utils import get_shanghai_time


SEMANTIC_ARTIFACT_SCHEMA_VERSION = "business_profile_semantic_artifact.v1"
SEMANTIC_ARTIFACT_STATUSES = {
    "received",
    "conversion_pending",
    "converted",
    "rejected",
    "replayed",
}
MAX_SEMANTIC_RESPONSE_BYTES = 256_000
MAX_EVIDENCE_IDS = 128


@dataclass(frozen=True)
class SemanticArtifactIdentity:
    instrument_id: str
    source_document_id: str
    document_hash: str
    report_period: str
    field_family: str
    evidence_scope_hash: str
    input_hash: str
    prompt_version: str
    schema_version: str


class BusinessProfileSemanticArtifactRepository:
    """Stores model output before conversion and replays only exact identities."""

    def __init__(self, storage: Any) -> None:
        self.storage = storage

    def receive(
        self,
        identity: SemanticArtifactIdentity,
        *,
        response: Mapping[str, Any],
        response_hash: str,
        evidence_ids: Sequence[str],
        model_profile: Optional[str] = None,
        actual_model: Optional[str] = None,
        usage: Optional[Mapping[str, Any]] = None,
        authority: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        response_json = _bounded_json(response, "semantic response")
        evidence = tuple(dict.fromkeys(str(item) for item in evidence_ids if str(item)))
        if len(evidence) > MAX_EVIDENCE_IDS:
            raise ValueError("semantic artifact evidence scope exceeds bound")
        expected_response_hash = _sha256(response_json)
        supplied_hash = str(response_hash or "").strip()
        if supplied_hash and len(supplied_hash) == 64 and supplied_hash != expected_response_hash:
            # Gateway hashes may be over raw wire JSON; preserve both without
            # weakening the content-addressed artifact identity.
            authority = {
                **dict(authority or {}),
                "gateway_response_hash": supplied_hash,
            }
        canonical_response_hash = expected_response_hash
        artifact_id = "bp-semantic-artifact-" + _stable_hash(
            {
                **identity.__dict__,
                "response_hash": canonical_response_hash,
            }
        )[:24]
        now = get_shanghai_time().isoformat()
        params = (
            artifact_id,
            identity.instrument_id,
            identity.source_document_id,
            identity.document_hash,
            identity.report_period,
            identity.field_family,
            identity.evidence_scope_hash,
            identity.input_hash,
            canonical_response_hash,
            identity.prompt_version,
            identity.schema_version,
            model_profile,
            actual_model,
            response_json,
            _bounded_json(list(evidence), "evidence ids"),
            _bounded_json(dict(usage or {}), "usage"),
            _bounded_json(dict(authority or {}), "authority"),
            now,
            now,
        )
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT OR IGNORE INTO business_profile_semantic_artifacts (
                    artifact_id, instrument_id, source_document_id, document_hash,
                    report_period, field_family, evidence_scope_hash, input_hash,
                    response_hash, prompt_version, schema_version, model_profile,
                    actual_model, response_json, evidence_ids_json, usage_json,
                    authority_json, received_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                params,
            )
            row = conn.execute(
                "SELECT * FROM business_profile_semantic_artifacts "
                "WHERE instrument_id = ? AND source_document_id = ? "
                "AND field_family = ? AND input_hash = ? AND prompt_version = ? "
                "AND schema_version = ? AND response_hash = ?",
                (
                    identity.instrument_id,
                    identity.source_document_id,
                    identity.field_family,
                    identity.input_hash,
                    identity.prompt_version,
                    identity.schema_version,
                    canonical_response_hash,
                ),
            ).fetchone()
            if row is None:
                conn.rollback()
                raise RuntimeError("semantic artifact receipt was not persisted")
            self._append_event_in_transaction(
                conn,
                str(row["artifact_id"]),
                "received",
                reason_code="model_response_validated",
            )
            conn.commit()
        return self._decode(dict(row))

    def find_replay(self, identity: SemanticArtifactIdentity) -> Optional[dict[str, Any]]:
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            row = conn.execute(
                """
                SELECT a.*,
                       (SELECT status FROM business_profile_semantic_artifact_events e
                        WHERE e.artifact_id = a.artifact_id
                        ORDER BY e.created_at DESC, e.rowid DESC LIMIT 1) latest_status,
                       (SELECT reason_code FROM business_profile_semantic_artifact_events e
                        WHERE e.artifact_id = a.artifact_id
                        ORDER BY e.created_at DESC, e.rowid DESC LIMIT 1) latest_reason_code
                FROM business_profile_semantic_artifacts a
                WHERE instrument_id = ? AND source_document_id = ?
                  AND document_hash = ? AND report_period = ? AND field_family = ?
                  AND evidence_scope_hash = ? AND input_hash = ?
                  AND prompt_version = ? AND schema_version = ?
                ORDER BY received_at DESC, a.rowid DESC LIMIT 1
                """,
                (
                    identity.instrument_id,
                    identity.source_document_id,
                    identity.document_hash,
                    identity.report_period,
                    identity.field_family,
                    identity.evidence_scope_hash,
                    identity.input_hash,
                    identity.prompt_version,
                    identity.schema_version,
                ),
            ).fetchone()
        if row is None:
            return None
        latest_status = str(row["latest_status"] or "")
        latest_reason = str(row["latest_reason_code"] or "")
        # A failed conversion is not a reusable semantic result.  The only
        # pending state that may be replayed is one explicitly reopened by a
        # governed unit-rule change; all other pending states must be freshly
        # extracted instead of looping over the same bad response.
        replayable = latest_status in {"received", "converted", "replayed"} or (
            latest_status == "conversion_pending"
            and latest_reason.startswith("unit_rule_")
        )
        if not replayable:
            return None
        return self._decode(dict(row))

    def mark(
        self,
        artifact_id: str,
        status: str,
        *,
        unit_catalog_version: Optional[str] = None,
        runtime_version: Optional[str] = None,
        reason_code: Optional[str] = None,
        saved_tokens: Optional[Mapping[str, int]] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> str:
        if status not in SEMANTIC_ARTIFACT_STATUSES:
            raise ValueError(f"unsupported semantic artifact status: {status}")
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            exists = conn.execute(
                "SELECT 1 FROM business_profile_semantic_artifacts WHERE artifact_id = ?",
                (artifact_id,),
            ).fetchone()
            if exists is None:
                conn.rollback()
                raise ValueError(f"unknown semantic artifact: {artifact_id}")
            event_id = self._append_event_in_transaction(
                conn,
                artifact_id,
                status,
                unit_catalog_version=unit_catalog_version,
                runtime_version=runtime_version,
                reason_code=reason_code,
                saved_tokens=saved_tokens,
                metadata=metadata,
            )
            conn.commit()
        return event_id

    @staticmethod
    def _append_event_in_transaction(
        conn: Any,
        artifact_id: str,
        status: str,
        *,
        unit_catalog_version: Optional[str] = None,
        runtime_version: Optional[str] = None,
        reason_code: Optional[str] = None,
        saved_tokens: Optional[Mapping[str, int]] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> str:
        now = get_shanghai_time().isoformat()
        payload = {
            "artifact_id": artifact_id,
            "status": status,
            "unit_catalog_version": unit_catalog_version,
            "runtime_version": runtime_version,
            "reason_code": reason_code,
            "metadata": dict(metadata or {}),
        }
        event_id = "bp-semantic-event-" + _stable_hash(payload)[:24]
        tokens = dict(saved_tokens or {})
        conn.execute(
            """
            INSERT OR IGNORE INTO business_profile_semantic_artifact_events (
                event_id, artifact_id, status, unit_catalog_version,
                runtime_version, reason_code, saved_input_tokens,
                saved_output_tokens, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                artifact_id,
                status,
                unit_catalog_version,
                runtime_version,
                reason_code,
                int(tokens.get("input_tokens") or 0),
                int(tokens.get("output_tokens") or 0),
                _bounded_json(dict(metadata or {}), "event metadata"),
                now,
            ),
        )
        return event_id

    @staticmethod
    def _decode(row: dict[str, Any]) -> dict[str, Any]:
        for key in ("response_json", "evidence_ids_json", "usage_json", "authority_json"):
            value = row.pop(key, None)
            row[key.removesuffix("_json")] = json.loads(value or "{}")
        return row


def _bounded_json(value: Any, label: str) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if len(payload.encode("utf-8")) > MAX_SEMANTIC_RESPONSE_BYTES:
        raise ValueError(f"{label} exceeds persistence bound")
    return payload


def _stable_hash(value: Any) -> str:
    return _sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    )


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
