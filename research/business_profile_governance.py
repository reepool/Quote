"""Point-in-time governance for company business profiles and commodity exposure."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import date
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from research.business_profile_temporal import (
    BusinessProfileTemporalClass,
    get_business_profile_temporal_policy,
    get_business_profile_supersession_column,
)
from research.business_profile_fact_catalog import load_business_fact_catalog
from research.business_profile_product_catalog import (
    load_business_product_catalog,
    normalize_product_alias,
)
from utils.date_utils import get_shanghai_time


BUSINESS_PROFILE_SCHEMA_VERSION = "company_business_profile.v2"
BUSINESS_PROFILE_SCORE_VERSION = "business_profile_model_score.v1"
BUSINESS_PROFILE_MEASUREMENT_CONTRACT_VERSION = "business_profile_measurements.v1"
REVIEW_STATUSES = {"candidate", "held", "approved", "rejected", "superseded"}
NON_CANDIDATE_REVIEW_STATUSES = REVIEW_STATUSES - {"candidate"}
MAX_BUSINESS_PROFILE_BULK_RECORDS = 5000
TERMINAL_REPLAY_PROVENANCE_FIELDS = {
    "review_status",
    "reviewed_by",
    "reviewed_at",
    "run_id",
    "parser_version",
    "extraction_method",
    "confidence",
    "lineage_hash",
    "metadata_json",
    "created_at",
    "updated_at",
}
MATERIAL_PROFILE_EVENT_TYPES = {
    "reverse_merger",
    "major_asset_restructuring",
    "business_acquisition",
    "business_disposal",
    "control_change",
    "principal_business_change",
}


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True)


def _json_loads(value: Optional[str], default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def _date_key(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text[:10] if len(text) >= 10 else None


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()


def _derive_profile_and_market_link_status(
    *,
    approved_company_fact_count: int,
    approved_exposures: Sequence[Mapping[str, Any]],
    executable_company_mappings: Sequence[Mapping[str, Any]],
) -> tuple[str, str, List[str]]:
    """Keep semantic profile readiness independent from market enrichment."""

    exposure_count = len(approved_exposures)
    executable_count = len(executable_company_mappings)
    if not exposure_count:
        market_status = "not_applicable"
    elif executable_count == exposure_count:
        market_status = "direct_linked"
    elif executable_count:
        market_status = "partial"
    else:
        market_status = "unlinked"
    linked_ids = {
        str(item.get("source_exposure_id") or "")
        for item in executable_company_mappings
    }
    unresolved_ids = [
        str(item.get("exposure_id"))
        for item in approved_exposures
        if str(item.get("exposure_id") or "") not in linked_ids
    ]
    profile_status = "ready" if approved_company_fact_count else "not_ready"
    return profile_status, market_status, unresolved_ids


def _build_business_profile_measurement_contract(
    operating_facts: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """Describe measurement authority and legacy activity linkage for API clients."""

    fact_count = len(operating_facts)
    activity_derived_facts = [
        fact
        for fact in operating_facts
        if isinstance(fact.get("metadata"), dict)
        and (
            bool(str(fact["metadata"].get("source_activity_id") or "").strip())
            or fact["metadata"].get("measurement_authority")
            == "llm_source_fields_program_normalized"
        )
    ]
    activity_derived_count = len(activity_derived_facts)
    linked_count = sum(
        bool(
            str(
                (fact.get("metadata") or {}).get("source_activity_id") or ""
                if isinstance(fact.get("metadata"), dict)
                else ""
            ).strip()
        )
        for fact in activity_derived_facts
    )
    if activity_derived_count == 0:
        linkage_status = "not_applicable"
    elif linked_count == activity_derived_count:
        linkage_status = "linked"
    elif linked_count:
        linkage_status = "partially_linked"
    else:
        linkage_status = "unlinked"
    return {
        "contract_version": BUSINESS_PROFILE_MEASUREMENT_CONTRACT_VERSION,
        "authoritative_measurements_path": (
            "company_specific_profile.operating_facts"
        ),
        "activity_measurement_role": "compatibility_projection",
        "operating_fact_activity_link_field": "metadata.source_activity_id",
        "operating_fact_count": fact_count,
        "activity_derived_operating_fact_count": activity_derived_count,
        "linked_activity_derived_operating_fact_count": linked_count,
        "standalone_operating_fact_count": fact_count - activity_derived_count,
        "linkage_status": linkage_status,
    }


def select_current_business_profile_activities(
    activities: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Prefer the most normalized version of an exact filing activity."""

    def rank(activity: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            bool(str(activity.get("object_id") or "").strip()),
            str(activity.get("knowledge_from") or ""),
            int(activity.get("version") or 0),
            str(activity.get("updated_at") or ""),
            str(activity.get("activity_id") or ""),
        )

    selected: Dict[tuple[Any, ...], Mapping[str, Any]] = {}
    for activity in activities:
        key = (
            str(activity.get("instrument_id") or ""),
            str(activity.get("report_period") or ""),
            str(activity.get("evidence_id") or ""),
            str(activity.get("subject_scope") or ""),
            str(activity.get("action") or ""),
            str(activity.get("object_type") or ""),
            normalize_product_alias(activity.get("object_raw")),
            str(activity.get("segment_id") or ""),
            str(activity.get("geography") or ""),
            activity.get("value"),
            str(activity.get("unit") or ""),
            activity.get("share"),
            str(activity.get("business_regime_id") or ""),
        )
        current = selected.get(key)
        if current is None or rank(activity) > rank(current):
            selected[key] = activity
    return [dict(item) for item in selected.values()]


def _collapse_identical_bundle_records(
    record_type: str,
    prepared: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Collapse exact duplicates while failing closed on same-key conflicts."""

    unique: Dict[str, Dict[str, Any]] = {}
    ordered: List[Dict[str, Any]] = []
    for item in prepared:
        primary_key = str(item["pk_value"])
        existing = unique.get(primary_key)
        if existing is None:
            unique[primary_key] = item
            ordered.append(item)
            continue
        if (
            existing["status"] != item["status"]
            or existing["payload"] != item["payload"]
        ):
            raise ValueError(
                "conflicting business profile primary key in bundle: "
                f"{record_type}:{primary_key}"
            )
    return ordered


def build_empty_business_profile_context(
    instrument_id: str,
    *,
    as_of_date: str,
    warning: str = "company_business_profile_missing",
) -> Dict[str, Any]:
    """Return a stable fail-closed context when profile storage is unavailable."""
    payload: Dict[str, Any] = {
        "schema_version": BUSINESS_PROFILE_SCHEMA_VERSION,
        "status": "not_ready",
        "market_link_status": "not_applicable",
        "instrument_id": instrument_id,
        "data_available_cutoff": _date_key(as_of_date) or str(as_of_date),
        "industry_default_profile": {},
        "company_specific_profile": {
            "business_regime": None,
            "segments": [],
            "operating_facts": [],
            "activities": [],
            "value_chain_roles": [],
            "supply_chain_relationships": [],
            "commodity_exposure_facts": [],
            "commodity_exposures": [],
        },
        "measurement_contract": _build_business_profile_measurement_contract([]),
        "segment_profiles": [],
        "approved_exposures": [],
        "candidate_exposures": [],
        "candidate_facts": {},
        "exceptions": [],
        "profile_lifecycle": {
            "active_regime": None,
            "approved_regimes": [],
            "approved_events": [],
            "candidate_regimes": [],
            "candidate_events": [],
        },
        "executable_exposure_mappings": [],
        "model_scores": {
            "score_version": BUSINESS_PROFILE_SCORE_VERSION,
            "industry_model_score": 0.0,
            "company_model_score": 0.0,
            "components": {},
        },
        "model_recommendation": "industry_default",
        "conflicts": [],
        "warnings": [warning],
        "readiness": {
            "status": "not_ready",
            "approved_company_fact_count": 0,
            "approved_company_exposure_count": 0,
            "active_business_regime_id": None,
            "approved_profile_event_count": 0,
            "industry_mapping_count": 0,
            "executable_mapping_count": 0,
            "input_gaps": [warning],
        },
    }
    payload["profile_version"] = _stable_hash(payload)[:24]
    payload["lineage_hash"] = _stable_hash(
        {"instrument_id": instrument_id, "as_of_date": as_of_date, "warning": warning}
    )
    return payload


class BusinessProfileRepository:
    """Repository for normalized company business-profile facts."""

    _TABLES: Dict[str, Dict[str, Any]] = {
        "evidence": {
            "table": "business_profile_evidence",
            "pk": "evidence_id",
            "columns": (
                "evidence_id", "instrument_id", "source_document_id",
                "source_institution", "source_tier", "document_type", "title",
                "source_url", "document_hash", "report_period", "publish_date",
                "data_available_date", "availability_quality", "page_number",
                "table_name", "section_path", "evidence_text_hash",
                "extraction_method", "parser_version", "ocr_status", "confidence",
                "review_status", "reviewed_by", "reviewed_at", "metadata_json",
                "created_at", "updated_at",
            ),
            "json": {"metadata_json"},
        },
        "events": {
            "table": "company_business_profile_events",
            "pk": "event_id",
            "columns": (
                "event_id", "instrument_id", "event_type", "event_date",
                "event_date_quality", "prior_regime_id", "resulting_regime_id",
                "materiality", "description", "evidence_id", "data_available_date",
                "confidence", "review_status", "version", "lineage_hash",
                "metadata_json", "created_at", "updated_at",
            ),
            "json": {"metadata_json"},
        },
        "regimes": {
            "table": "company_business_profile_regimes",
            "pk": "regime_id",
            "columns": (
                "regime_id", "regime_key", "instrument_id", "regime_name",
                "regime_type", "valid_from", "valid_to", "knowledge_from",
                "knowledge_to", "trigger_event_id", "evidence_id",
                "data_available_date", "confidence", "review_status", "version",
                "lineage_hash", "metadata_json", "created_at", "updated_at",
            ),
            "json": {"metadata_json"},
        },
        "segments": {
            "table": "company_business_segments",
            "pk": "record_id",
            "columns": (
                "record_id", "instrument_id", "report_period", "segment_id",
                "segment_name_raw", "segment_name_normalized", "segment_type",
                "revenue", "revenue_share", "segment_cost", "cost_share",
                "segment_profit", "profit_share", "gross_margin", "segment_assets",
                "currency", "consolidation_scope", "geography", "source_document_id",
                "evidence_id", "data_available_date", "extraction_method", "confidence",
                "review_status", "valid_from", "valid_to", "business_regime_id",
                "knowledge_from", "knowledge_to", "supersedes_record_id", "version",
                "lineage_hash", "metadata_json", "created_at", "updated_at",
            ),
            "json": {"metadata_json"},
        },
        "operating_facts": {
            "table": "company_operating_facts",
            "pk": "record_id",
            "columns": (
                "record_id", "instrument_id", "report_period", "segment_id",
                "project_id", "fact_type", "value_raw", "unit_raw",
                "value_normalized", "unit_normalized", "fact_scope", "currency",
                "equity_basis", "evidence_id", "data_available_date", "confidence",
                "review_status", "valid_from", "valid_to", "business_regime_id",
                "knowledge_from", "knowledge_to", "supersedes_record_id", "version",
                "lineage_hash", "metadata_json", "created_at", "updated_at",
            ),
            "json": {"metadata_json"},
        },
        "activities": {
            "table": "company_business_activities",
            "pk": "activity_id",
            "columns": (
                "activity_id", "instrument_id", "report_period", "subject_scope",
                "action", "object_type", "object_raw", "object_id", "segment_id",
                "geography", "value", "unit", "share", "evidence_id", "run_id",
                "data_available_date", "extraction_method", "confidence",
                "review_status", "valid_from", "valid_to", "business_regime_id",
                "knowledge_from", "knowledge_to", "supersedes_activity_id",
                "version", "lineage_hash", "metadata_json", "created_at", "updated_at",
            ),
            "json": {"metadata_json"},
        },
        "value_chain_roles": {
            "table": "company_value_chain_roles",
            "pk": "record_id",
            "columns": (
                "record_id", "instrument_id", "report_period", "segment_id", "role",
                "materiality", "revenue_share", "mapping_basis", "evidence_id",
                "data_available_date", "confidence", "review_status", "valid_from",
                "valid_to", "business_regime_id", "knowledge_from", "knowledge_to",
                "supersedes_record_id", "version", "lineage_hash", "metadata_json",
                "created_at", "updated_at",
            ),
            "json": {"metadata_json"},
        },
        "relationships": {
            "table": "company_supply_chain_relationships",
            "pk": "relationship_id",
            "columns": (
                "relationship_id", "instrument_id", "report_period",
                "relationship_type", "direction", "counterparty_name_raw",
                "counterparty_name_normalized", "counterparty_entity_id",
                "resolution_basis", "anonymous", "scope_type", "scope_id",
                "object_raw", "object_id", "disclosed_value", "disclosed_unit",
                "disclosed_share", "evidence_id", "run_id", "data_available_date",
                "confidence", "review_status", "valid_from", "valid_to",
                "business_regime_id", "knowledge_from", "knowledge_to",
                "supersedes_relationship_id", "version", "lineage_hash",
                "metadata_json", "created_at", "updated_at",
            ),
            "json": {"metadata_json"},
        },
        "exposure_facts": {
            "table": "company_commodity_exposure_facts",
            "pk": "fact_id",
            "columns": (
                "fact_id", "instrument_id", "report_period", "activity_id",
                "segment_id", "exposure_fact_type", "object_raw", "product_id",
                "value_raw", "unit_raw", "value_normalized", "unit_normalized",
                "share", "fact_scope", "evidence_id", "run_id",
                "data_available_date", "confidence", "review_status", "valid_from",
                "valid_to", "business_regime_id", "knowledge_from", "knowledge_to",
                "supersedes_fact_id", "version", "lineage_hash", "metadata_json",
                "created_at", "updated_at",
            ),
            "json": {"metadata_json"},
        },
        "exposure_assumptions": {
            "table": "company_commodity_exposure_assumptions",
            "pk": "assumption_id",
            "columns": (
                "assumption_id", "instrument_id", "scope_type", "scope_id",
                "assumption_type", "assumption_value", "unit", "method",
                "sample_start", "sample_end", "evidence_id", "run_id",
                "data_available_date", "confidence", "review_status",
                "effective_from", "effective_to", "knowledge_from", "knowledge_to",
                "supersedes_assumption_id", "version", "lineage_hash", "metadata_json",
                "created_at", "updated_at",
            ),
            "json": {"metadata_json"},
        },
        "exposures": {
            "table": "company_commodity_exposures",
            "pk": "exposure_id",
            "columns": (
                "exposure_id", "instrument_id", "report_period", "scope_type",
                "scope_id", "commodity_id", "exposure_role", "direction", "materiality",
                "mapping_basis", "price_series_id", "spread_definition_id", "lag_days",
                "pass_through_score", "hedge_adjustment", "evidence_id",
                "data_available_date", "confidence", "review_status", "effective_from",
                "effective_to", "business_regime_id", "knowledge_from", "knowledge_to",
                "fact_ids_json", "mapping_ids_json", "assumption_ids_json",
                "direction_rule_id", "build_policy_version", "build_policy_hash",
                "component_lineage_hash", "legacy_compatibility_status",
                "supersedes_exposure_id", "version", "lineage_hash", "metadata_json",
                "created_at", "updated_at",
            ),
            "json": {
                "fact_ids_json", "mapping_ids_json", "assumption_ids_json",
                "metadata_json",
            },
        },
    }

    def __init__(self, storage: Any):
        self.storage = storage

    def upsert(self, record_type: str, record: Dict[str, Any]) -> str:
        """Idempotently upsert one governed record by its stable primary key."""
        prepared = self._prepare_record(record_type, record)
        spec = prepared["spec"]
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            try:
                existing = conn.execute(
                    f"SELECT review_status FROM {spec['table']} "
                    f"WHERE {spec['pk']} = ?",
                    (prepared["pk_value"],),
                ).fetchone()
                self._validate_write_state(
                    record_type=record_type,
                    pk_value=prepared["pk_value"],
                    incoming_status=prepared["status"],
                    existing_status=(
                        str(existing["review_status"]) if existing is not None else None
                    ),
                )
                self._validate_temporal_state(conn, record_type, prepared)
                conn.execute(self._upsert_sql(spec), prepared["values"])
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return prepared["pk_value"]

    def _prepare_record(
        self,
        record_type: str,
        record: Dict[str, Any],
        *,
        now: Optional[str] = None,
    ) -> Dict[str, Any]:
        spec = self._TABLES.get(record_type)
        if spec is None:
            raise ValueError(f"unsupported business profile record type: {record_type}")
        payload = dict(record)
        allowed_fields = set(spec["columns"])
        for json_column in spec["json"]:
            if json_column.endswith("_json"):
                allowed_fields.discard(json_column)
                allowed_fields.add(json_column[:-5])
        unknown_fields = sorted(set(payload) - allowed_fields)
        if unknown_fields:
            raise ValueError(
                f"unknown fields for business profile {record_type}: {unknown_fields}"
            )
        pk = spec["pk"]
        if not payload.get(pk):
            raise ValueError(f"{pk} is required")
        if not payload.get("instrument_id"):
            raise ValueError("instrument_id is required")
        status = str(payload.get("review_status") or "candidate").lower()
        if status not in REVIEW_STATUSES:
            raise ValueError(f"unsupported review_status: {status}")
        payload["review_status"] = status
        payload.setdefault("version", 1)
        if record_type == "regimes":
            payload.setdefault("regime_key", payload.get("regime_id"))
        if "knowledge_from" in spec["columns"]:
            payload.setdefault("knowledge_from", payload.get("data_available_date"))
            if not payload.get("knowledge_from"):
                raise ValueError("knowledge_from or data_available_date is required")
            self._validate_interval(
                payload.get("knowledge_from"),
                payload.get("knowledge_to"),
                "knowledge",
            )
        temporal_policy = get_business_profile_temporal_policy(record_type)
        if (
            temporal_policy.validity_start_field
            and temporal_policy.validity_start_field in spec["columns"]
        ):
            interval_label = temporal_policy.validity_start_field.rsplit("_", 1)[0]
            self._validate_interval(
                payload.get(temporal_policy.validity_start_field),
                payload.get(temporal_policy.validity_end_field or ""),
                interval_label,
            )
        prepared_at = now or get_shanghai_time().isoformat()
        payload.setdefault("created_at", prepared_at)
        payload["updated_at"] = prepared_at
        if "lineage_hash" in spec["columns"] and not payload.get("lineage_hash"):
            payload["lineage_hash"] = _stable_hash(
                {key: value for key, value in payload.items() if key not in {"created_at", "updated_at"}}
            )
        columns = list(spec["columns"])
        values: List[Any] = []
        for column in columns:
            value = payload.get(column)
            if column in spec["json"]:
                value = _json_dumps(payload.get(column[:-5]) if column.endswith("_json") else value)
            values.append(value)
        return {
            "spec": spec,
            "pk_value": str(payload[pk]),
            "status": status,
            "payload": payload,
            "values": values,
        }

    @staticmethod
    def _upsert_sql(spec: Dict[str, Any]) -> str:
        pk = spec["pk"]
        columns = list(spec["columns"])
        updates = [
            f"{column}=excluded.{column}"
            for column in columns
            if column not in {pk, "created_at"}
        ]
        return f"""
            INSERT INTO {spec['table']} ({', '.join(columns)})
            VALUES ({', '.join('?' for _ in columns)})
            ON CONFLICT({pk}) DO UPDATE SET {', '.join(updates)}
            WHERE {spec['table']}.review_status = 'candidate'
              AND excluded.review_status = 'candidate'
        """

    @staticmethod
    def _validate_write_state(
        *,
        record_type: str,
        pk_value: str,
        incoming_status: str,
        existing_status: Optional[str],
    ) -> None:
        if existing_status is None and incoming_status in NON_CANDIDATE_REVIEW_STATUSES:
            raise ValueError(
                "new business profile records must be inserted as candidate; "
                "review status transitions require BusinessProfileReviewService: "
                f"{record_type}:{pk_value}"
            )
        if existing_status == "candidate" and incoming_status != "candidate":
            raise ValueError(
                "review status transitions require BusinessProfileReviewService: "
                f"{record_type}:{pk_value}"
            )

    def upsert_many(self, record_type: str, records: Iterable[Dict[str, Any]]) -> int:
        raw_records = list(records)
        if not raw_records:
            return 0
        if len(raw_records) > MAX_BUSINESS_PROFILE_BULK_RECORDS:
            raise ValueError(
                "business profile bulk batch exceeds limit: "
                f"{len(raw_records)} > {MAX_BUSINESS_PROFILE_BULK_RECORDS}"
            )
        prepared_at = get_shanghai_time().isoformat()
        prepared = [
            self._prepare_record(record_type, record, now=prepared_at)
            for record in raw_records
        ]
        primary_keys = [item["pk_value"] for item in prepared]
        if len(primary_keys) != len(set(primary_keys)):
            raise ValueError("duplicate business profile primary key in bulk batch")
        self._validate_prepared_batch_temporal(record_type, prepared)
        spec = prepared[0]["spec"]
        prepared_by_pk = {item["pk_value"]: item["payload"] for item in prepared}
        existing_statuses: Dict[str, str] = {}
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            try:
                for offset in range(0, len(primary_keys), 500):
                    chunk = primary_keys[offset : offset + 500]
                    placeholders = ", ".join("?" for _ in chunk)
                    rows = conn.execute(
                        f"SELECT {spec['pk']}, review_status FROM {spec['table']} "
                        f"WHERE {spec['pk']} IN ({placeholders})",
                        chunk,
                    ).fetchall()
                    existing_statuses.update(
                        {
                            str(row[spec["pk"]]): str(row["review_status"])
                            for row in rows
                        }
                    )
                for item in prepared:
                    self._validate_write_state(
                        record_type=record_type,
                        pk_value=item["pk_value"],
                        incoming_status=item["status"],
                        existing_status=existing_statuses.get(item["pk_value"]),
                    )
                    self._validate_temporal_state(
                        conn,
                        record_type,
                        item,
                        prepared_by_pk=prepared_by_pk,
                    )
                conn.executemany(
                    self._upsert_sql(spec),
                    [item["values"] for item in prepared],
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return len(prepared)

    def persist_document_field_family_bundle(
        self,
        *,
        run: Dict[str, Any],
        records_by_type: Dict[str, Iterable[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """Atomically persist one completed document and field-family bundle."""

        allowed_types = {
            "evidence",
            "segments",
            "operating_facts",
            "activities",
            "relationships",
            "exposure_facts",
        }
        unknown_types = sorted(set(records_by_type) - allowed_types)
        if unknown_types:
            raise ValueError(f"unsupported bundle record types: {unknown_types}")
        run_payload = dict(run)
        required_run_fields = {
            "run_id",
            "instrument_id",
            "source_document_id",
            "field_family",
            "bundle_hash",
        }
        missing_run_fields = sorted(
            key for key in required_run_fields if not run_payload.get(key)
        )
        if missing_run_fields:
            raise ValueError(f"business profile bundle run missing: {missing_run_fields}")
        fact_catalog_version = str(
            run_payload.get("fact_catalog_version") or ""
        ).strip()
        if (
            fact_catalog_version
            and fact_catalog_version != load_business_fact_catalog().catalog_version
        ):
            raise ValueError("business profile bundle fact catalog version mismatch")
        product_catalog_version = str(
            run_payload.get("product_catalog_version") or ""
        ).strip()
        if (
            product_catalog_version
            and product_catalog_version
            != load_business_product_catalog().catalog_version
        ):
            raise ValueError("business profile bundle product catalog version mismatch")
        prepared_at = get_shanghai_time().isoformat()
        run_metadata = run_payload.get("metadata") or {}
        # The async runtime stores the policy in metadata.  Accept the
        # top-level form as well so replay callers cannot accidentally lose
        # the explicit reuse/replace contract while reconstructing a bundle.
        result_policy_value = run_metadata.get(
            "result_policy", run_payload.get("result_policy")
        )
        # Older direct repository callers omit the policy and retain the
        # original strict replay contract. The async production runtime always
        # supplies it explicitly, so only that path gets conservative reuse.
        result_policy = str(result_policy_value or "reuse").strip().lower()
        reuse_requested = result_policy_value is not None and result_policy == "reuse"
        prepared_by_type: Dict[str, List[Dict[str, Any]]] = {}
        total_records = 0
        for record_type, raw_records in records_by_type.items():
            rows = list(raw_records)
            total_records += len(rows)
            if total_records > MAX_BUSINESS_PROFILE_BULK_RECORDS:
                raise ValueError("business profile bundle exceeds bulk record limit")
            prepared = [
                self._prepare_record(record_type, row, now=prepared_at)
                for row in rows
            ]
            prepared = _collapse_identical_bundle_records(record_type, prepared)
            self._validate_prepared_batch_temporal(
                record_type,
                prepared,
                allow_reuse_conflicts=reuse_requested,
            )
            prepared_by_type[record_type] = prepared
        # A replace run is an explicit request to publish a new observation for
        # the same report flow. Link it to the current candidate/approved
        # version so temporal governance can retain history without treating
        # the rerun as an accidental duplicate.
        replace_requested = result_policy == "replace"
        run_id = str(run_payload["run_id"])
        for record_type, prepared in prepared_by_type.items():
            if record_type == "evidence":
                continue
            for item in prepared:
                payload = item["payload"]
                if "run_id" in item["spec"]["columns"]:
                    if str(payload.get("run_id") or "") != run_id:
                        raise ValueError(
                            f"bundle record run_id mismatch: {record_type}:{item['pk_value']}"
                        )
                evidence_id = str(payload.get("evidence_id") or "").strip()
                if not evidence_id:
                    raise ValueError(
                        f"bundle record evidence_id is required: {record_type}:{item['pk_value']}"
                    )
        counts = {
            "evidence_count": sum(
                1
                for item in prepared_by_type.get("evidence", [])
                if not item.get("skip_write")
            ),
            "fact_count": sum(
                sum(
                    1
                    for item in prepared_by_type.get(key, [])
                    if not item.get("skip_write")
                )
                for key in ("segments", "operating_facts", "exposure_facts")
            ),
            "activity_count": sum(
                1
                for item in prepared_by_type.get("activities", [])
                if not item.get("skip_write")
            ),
            "relationship_count": sum(
                1
                for item in prepared_by_type.get("relationships", [])
                if not item.get("skip_write")
            ),
        }
        # Keep the durable run manifest aligned with rows that will actually
        # be written. Skipped reuse conflicts remain available in the raw
        # semantic artifact, but must not make a later family replay reject
        # an otherwise complete run for missing record ids.
        run_metadata = dict(run_payload.get("metadata") or {})
        record_ids = dict(run_metadata.get("record_ids") or {})
        for record_type, prepared in prepared_by_type.items():
            if record_type == "evidence":
                continue
            record_ids[record_type] = [
                item["pk_value"] for item in prepared if not item.get("skip_write")
            ]
        run_metadata["record_ids"] = record_ids
        run_metadata["evidence_ids"] = [
            item["pk_value"]
            for item in prepared_by_type.get("evidence", [])
            if not item.get("skip_write")
        ]
        run_payload["metadata"] = run_metadata
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            try:
                if replace_requested:
                    self._attach_replace_successors(
                        conn, prepared_by_type=prepared_by_type
                    )
                if conn.execute(
                    "SELECT 1 FROM business_profile_semantic_runs WHERE run_id = ?",
                    (run_id,),
                ).fetchone():
                    raise ValueError(f"business profile semantic run already exists: {run_id}")
                bundle_evidence = {
                    item["pk_value"]: item["payload"]
                    for item in prepared_by_type.get("evidence", [])
                }
                referenced_evidence_ids = sorted(
                    {
                        str(item["payload"].get("evidence_id") or "")
                        for record_type, prepared in prepared_by_type.items()
                        if record_type != "evidence"
                        for item in prepared
                        if item["payload"].get("evidence_id")
                    }
                    - set(bundle_evidence)
                )
                database_evidence: Dict[str, Dict[str, Any]] = {}
                for offset in range(0, len(referenced_evidence_ids), 500):
                    chunk = referenced_evidence_ids[offset : offset + 500]
                    placeholders = ", ".join("?" for _ in chunk)
                    rows = conn.execute(
                        "SELECT evidence_id, instrument_id, source_document_id "
                        "FROM business_profile_evidence "
                        f"WHERE evidence_id IN ({placeholders})",
                        chunk,
                    ).fetchall()
                    database_evidence.update(
                        {str(row["evidence_id"]): dict(row) for row in rows}
                    )
                for record_type, prepared in prepared_by_type.items():
                    spec = self._TABLES[record_type]
                    prepared_by_pk = {
                        item["pk_value"]: item["payload"] for item in prepared
                    }
                    for item in prepared:
                        existing = conn.execute(
                            f"SELECT * FROM {spec['table']} "
                            f"WHERE {spec['pk']} = ?",
                            (item["pk_value"],),
                        ).fetchone()
                        self._validate_write_state(
                            record_type=record_type,
                            pk_value=item["pk_value"],
                            incoming_status=item["status"],
                            existing_status=(
                                str(existing["review_status"])
                                if existing is not None
                                else None
                            ),
                        )
                        if (
                            existing is not None
                            and str(existing["review_status"])
                            in NON_CANDIDATE_REVIEW_STATUSES
                        ):
                            existing_content_hash = self._terminal_content_hash(
                                spec,
                                dict(existing),
                            )
                            incoming_content_hash = self._terminal_content_hash(
                                spec,
                                dict(zip(spec["columns"], item["values"])),
                            )
                            if existing_content_hash != incoming_content_hash:
                                if reuse_requested:
                                    # Reuse must never replace an approved or
                                    # held fact because a replay produced a
                                    # different candidate identity. Keep the
                                    # governed row and complete the replay;
                                    # the differing candidate is retained in
                                    # the semantic artifact for diagnosis.
                                    item["skip_write"] = True
                                    item["reuse_conflict"] = True
                                else:
                                    raise ValueError(
                                        "business profile terminal-state race changed content: "
                                        f"{record_type}:{item['pk_value']}"
                                    )
                            item["skip_write"] = True
                        self._validate_temporal_state(
                            conn,
                            record_type,
                            item,
                            prepared_by_pk=prepared_by_pk,
                            allow_reuse_conflict=reuse_requested,
                        )
                        if record_type != "evidence":
                            evidence_id = str(
                                item["payload"].get("evidence_id") or ""
                            ).strip()
                            evidence_row = bundle_evidence.get(
                                evidence_id
                            ) or database_evidence.get(evidence_id)
                            if evidence_row is not None and evidence_row.get(
                                "instrument_id"
                            ) != item["payload"].get("instrument_id"):
                                raise ValueError(
                                    "bundle evidence instrument mismatch: "
                                    f"{record_type}:{evidence_id}"
                                )
                # Temporal reuse validation may mark a replay candidate as
                # skip_write (for example a zero-valued row shadowed by an
                # already approved non-zero fact).  Recompute counts after
                # validation so the run manifest reports rows actually
                # persisted, not rows merely prepared.
                counts = {
                    "evidence_count": sum(
                        1
                        for item in prepared_by_type.get("evidence", [])
                        if not item.get("skip_write")
                    ),
                    "fact_count": sum(
                        sum(
                            1
                            for item in prepared_by_type.get(key, [])
                            if not item.get("skip_write")
                        )
                        for key in ("segments", "operating_facts", "exposure_facts")
                    ),
                    "activity_count": sum(
                        1
                        for item in prepared_by_type.get("activities", [])
                        if not item.get("skip_write")
                    ),
                    "relationship_count": sum(
                        1
                        for item in prepared_by_type.get("relationships", [])
                        if not item.get("skip_write")
                    ),
                }
                for record_type in (
                    "evidence",
                    "segments",
                    "operating_facts",
                    "activities",
                    "relationships",
                    "exposure_facts",
                ):
                    prepared = prepared_by_type.get(record_type, [])
                    if prepared:
                        conn.executemany(
                            self._upsert_sql(prepared[0]["spec"]),
                            [
                                item["values"]
                                for item in prepared
                                if not item.get("skip_write")
                            ],
                        )
                conn.execute(
                    """
                    INSERT INTO business_profile_semantic_runs (
                        run_id, instrument_id, source_document_id, field_family,
                        status, bundle_hash, fact_catalog_version,
                        product_catalog_version, evidence_count, fact_count,
                        activity_count, relationship_count, error_code, metadata_json,
                        started_at, completed_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'completed', ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        run_payload["instrument_id"],
                        run_payload["source_document_id"],
                        run_payload["field_family"],
                        run_payload["bundle_hash"],
                        fact_catalog_version or None,
                        product_catalog_version or None,
                        counts["evidence_count"],
                        counts["fact_count"],
                        counts["activity_count"],
                        counts["relationship_count"],
                        _json_dumps(run_payload.get("metadata") or {}),
                        run_payload.get("started_at") or prepared_at,
                        prepared_at,
                        prepared_at,
                        prepared_at,
                    ),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return {"run_id": run_id, "status": "completed", **counts}

    def _attach_replace_successors(
        self,
        conn: Any,
        *,
        prepared_by_type: Mapping[str, Sequence[Dict[str, Any]]],
    ) -> None:
        """Attach replacement candidates to the current governed version.

        Only rows that would otherwise duplicate the temporal identity are
        linked. The incoming record remains a candidate; approved/held history
        is never modified or deleted, while an older candidate is superseded.
        """
        for record_type, prepared_rows in prepared_by_type.items():
            policy = get_business_profile_temporal_policy(record_type)
            pointer_column = get_business_profile_supersession_column(record_type)
            if pointer_column not in self._TABLES[record_type]["columns"]:
                continue
            for prepared in prepared_rows:
                payload = prepared["payload"]
                if payload.get(pointer_column):
                    continue
                clauses = [f"{field} IS ?" for field in policy.stable_identity_fields]
                values = [payload.get(field) for field in policy.stable_identity_fields]
                spec = prepared["spec"]
                rows = conn.execute(
                    f"SELECT * FROM {spec['table']} WHERE {' AND '.join(clauses)} "
                    "AND review_status IN ('candidate', 'held', 'approved') "
                    f"AND {spec['pk']} <> ? ORDER BY version DESC, updated_at DESC",
                    (*values, prepared["pk_value"]),
                ).fetchall()
                predecessor = None
                for row in rows:
                    existing = dict(row)
                    if self._temporal_versions_conflict(record_type, payload, existing):
                        predecessor = existing
                        break
                if predecessor is None:
                    continue
                payload[pointer_column] = str(predecessor[spec["pk"]])
                payload["version"] = max(
                    int(payload.get("version") or 1),
                    int(predecessor.get("version") or 0) + 1,
                )
                if str(predecessor.get("review_status") or "") == "candidate":
                    conn.execute(
                        f"UPDATE {spec['table']} SET review_status = 'superseded', "
                        "updated_at = ? WHERE "
                        f"{spec['pk']} = ? AND review_status = 'candidate'",
                        (get_shanghai_time().isoformat(), predecessor[spec["pk"]]),
                    )
                prepared["values"] = [
                    _json_dumps(
                        payload.get(column[:-5])
                        if column.endswith("_json")
                        else payload.get(column)
                    )
                    if column in spec["json"]
                    else payload.get(column)
                    for column in spec["columns"]
                ]

    @staticmethod
    def _terminal_content_hash(spec: Dict[str, Any], row: Dict[str, Any]) -> str:
        return _stable_hash(
            {
                column: row.get(column)
                for column in spec["columns"]
                if column not in TERMINAL_REPLAY_PROVENANCE_FIELDS
            }
        )

    def _validate_prepared_batch_temporal(
        self,
        record_type: str,
        prepared: Sequence[Dict[str, Any]],
        *,
        allow_reuse_conflicts: bool = False,
    ) -> None:
        policy = get_business_profile_temporal_policy(record_type)
        for index, left in enumerate(prepared):
            for right in prepared[index + 1 :]:
                if (
                    left["status"] == "candidate"
                    and right["status"] == "candidate"
                    and not allow_reuse_conflicts
                ):
                    continue
                if not self._same_stable_identity(
                    policy.stable_identity_fields,
                    left["payload"],
                    right["payload"],
                ):
                    continue
                if self._temporal_versions_conflict(
                    record_type,
                    left["payload"],
                    right["payload"],
                ):
                    if allow_reuse_conflicts and record_type in {
                        "operating_facts", "segments"
                    }:
                        winner = self._prefer_report_flow_candidate(left, right)
                        loser = right if winner is left else left
                        loser["skip_write"] = True
                        continue
                    raise ValueError(
                        "business profile temporal conflict within bulk batch: "
                        f"{record_type}:{left['pk_value']}:{right['pk_value']}"
                    )

    def _validate_temporal_state(
        self,
        conn: Any,
        record_type: str,
        prepared: Dict[str, Any],
        *,
        prepared_by_pk: Optional[Dict[str, Dict[str, Any]]] = None,
        allow_reuse_conflict: bool = False,
    ) -> None:
        if prepared.get("skip_write"):
            return
        policy = get_business_profile_temporal_policy(record_type)
        spec = prepared["spec"]
        payload = prepared["payload"]
        pointer_column = get_business_profile_supersession_column(record_type)
        pointer = (
            str(payload.get(pointer_column) or "").strip()
            if pointer_column in spec["columns"]
            else ""
        )
        if pointer == prepared["pk_value"]:
            raise ValueError("business profile record cannot supersede itself")
        if pointer:
            prior_row = conn.execute(
                f"SELECT * FROM {spec['table']} WHERE {spec['pk']} = ?",
                (pointer,),
            ).fetchone()
            prior = (
                dict(prior_row)
                if prior_row is not None
                else (prepared_by_pk or {}).get(pointer)
            )
            if prior is None:
                raise ValueError(f"superseded business profile record not found: {pointer}")
            if not self._same_stable_identity(
                policy.stable_identity_fields,
                payload,
                prior,
            ):
                raise ValueError("superseded business profile stable identity mismatch")
            if int(payload.get("version") or 0) <= int(prior.get("version") or 0):
                raise ValueError("superseding business profile version must increase")
        identity_clauses = [f"{field} IS ?" for field in policy.stable_identity_fields]
        identity_values = [payload.get(field) for field in policy.stable_identity_fields]
        rows = conn.execute(
            f"SELECT * FROM {spec['table']} WHERE {' AND '.join(identity_clauses)} "
            f"AND {spec['pk']} <> ? "
            "AND review_status IN ('held', 'approved')",
            [*identity_values, prepared["pk_value"]],
        ).fetchall()
        for row in rows:
            existing = dict(row)
            if pointer and str(existing.get(spec["pk"])) == pointer:
                continue
            if self._temporal_versions_conflict(record_type, payload, existing):
                if self._equivalent_report_flow_content(record_type, payload, existing):
                    # A replay can produce a new record id when evidence spans
                    # are regenerated, while the reported fact is unchanged.
                    # Link it to the governed version instead of treating the
                    # same fact as a temporal contradiction.
                    if pointer:
                        continue
                    payload[pointer_column] = str(existing[spec["pk"]])
                    payload["version"] = max(
                        int(payload.get("version") or 1),
                        int(existing.get("version") or 0) + 1,
                    )
                    prepared["values"] = [
                        _json_dumps(
                            payload.get(column[:-5])
                            if column.endswith("_json")
                            else payload.get(column)
                        )
                        if column in spec["json"]
                        else payload.get(column)
                        for column in spec["columns"]
                    ]
                    continue
                if allow_reuse_conflict and record_type in {
                    "operating_facts", "segments"
                }:
                    # Reuse is intentionally conservative: an already held or
                    # approved report-flow fact remains authoritative. The
                    # conflicting replay candidate stays in its semantic
                    # artifact and is retriable through an explicit replace
                    # run, instead of aborting the whole document bundle.
                    prepared["skip_write"] = True
                    return
                raise ValueError(
                    "business profile temporal conflict: "
                    f"{record_type}:{prepared['pk_value']}:{existing.get(spec['pk'])}"
                )

    @staticmethod
    def _equivalent_report_flow_content(
        record_type: str,
        left: Mapping[str, Any],
        right: Mapping[str, Any],
    ) -> bool:
        """Return whether two report-flow rows carry the same reported fact."""

        if record_type not in {"operating_facts", "segments"}:
            return False
        fields = (
            (
                "report_period", "segment_id", "project_id", "fact_type",
                "value_raw", "unit_raw", "value_normalized", "unit_normalized",
                "fact_scope", "currency", "equity_basis", "valid_from", "valid_to",
            )
            if record_type == "operating_facts"
            else (
                "report_period", "segment_id", "segment_type",
                "consolidation_scope", "revenue", "revenue_share", "segment_cost",
                "cost_share", "segment_profit", "profit_share", "gross_margin",
                "segment_assets", "currency", "geography", "valid_from", "valid_to",
            )
        )
        return all(left.get(field) == right.get(field) for field in fields)

    @staticmethod
    def _prefer_report_flow_candidate(
        left: Dict[str, Any], right: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Choose a deterministic winner for duplicate reuse candidates.

        A zero measurement is commonly emitted for an unfulfilled contract
        row alongside the fulfilled amount in the same annual-report table.
        Reuse keeps the non-zero reported fact and leaves the discarded row in
        the semantic artifact for audit. Other conflicts retain source order;
        explicit ``result_policy=replace`` is required to change history.
        """

        left_value = left["payload"].get("value_raw")
        right_value = right["payload"].get("value_raw")
        try:
            left_zero = float(left_value) == 0.0
            right_zero = float(right_value) == 0.0
        except (TypeError, ValueError):
            left_zero = right_zero = False
        return left if (not left_zero or right_zero) else right

    @staticmethod
    def _same_stable_identity(
        fields: Sequence[str],
        left: Dict[str, Any],
        right: Dict[str, Any],
    ) -> bool:
        return all(left.get(field) == right.get(field) for field in fields)

    @staticmethod
    def _temporal_versions_conflict(
        record_type: str,
        left: Dict[str, Any],
        right: Dict[str, Any],
    ) -> bool:
        policy = get_business_profile_temporal_policy(record_type)
        supersession_column = get_business_profile_supersession_column(record_type)
        left_pointer = str(left.get(supersession_column) or "")
        right_pointer = str(right.get(supersession_column) or "")
        left_id = str(
            left.get("record_id")
            or left.get("exposure_id")
            or left.get("fact_id")
            or left.get("assumption_id")
            or left.get("regime_id")
            or ""
        )
        right_id = str(
            right.get("record_id")
            or right.get("exposure_id")
            or right.get("fact_id")
            or right.get("assumption_id")
            or right.get("regime_id")
            or ""
        )
        if left_pointer == right_id or right_pointer == left_id:
            return False
        if policy.temporal_class == BusinessProfileTemporalClass.REPORT_FLOW:
            field = policy.observation_period_field or "report_period"
            return left.get(field) == right.get(field)
        if policy.temporal_class == BusinessProfileTemporalClass.EVENT:
            return False
        validity_overlap = BusinessProfileRepository._intervals_overlap(
            left.get(policy.validity_start_field or ""),
            left.get(policy.validity_end_field or ""),
            right.get(policy.validity_start_field or ""),
            right.get(policy.validity_end_field or ""),
        )
        knowledge_overlap = BusinessProfileRepository._intervals_overlap(
            left.get("knowledge_from") or left.get("data_available_date"),
            left.get("knowledge_to"),
            right.get("knowledge_from") or right.get("data_available_date"),
            right.get("knowledge_to"),
        )
        return validity_overlap and knowledge_overlap

    @staticmethod
    def _intervals_overlap(
        left_start: Any,
        left_end: Any,
        right_start: Any,
        right_end: Any,
    ) -> bool:
        left_start_key = _date_key(left_start) or "0001-01-01"
        left_end_key = _date_key(left_end) or "9999-12-31"
        right_start_key = _date_key(right_start) or "0001-01-01"
        right_end_key = _date_key(right_end) or "9999-12-31"
        return left_start_key < right_end_key and right_start_key < left_end_key

    def list_records(
        self,
        record_type: str,
        *,
        instrument_id: Optional[str] = None,
        review_status: Optional[str] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        page = self.list_records_page(
            record_type,
            instrument_id=instrument_id,
            review_status=review_status,
            page_size=limit,
        )
        return page["records"]

    def get_record(self, record_type: str, record_id: str) -> Optional[Dict[str, Any]]:
        """Return one governed record by its primary key without a history scan."""

        spec = self._TABLES.get(record_type)
        if spec is None:
            raise ValueError(f"unsupported business profile record type: {record_type}")
        key = str(record_id or "").strip()
        if not key:
            raise ValueError("business profile record_id is required")
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            row = conn.execute(
                f"SELECT * FROM {spec['table']} WHERE {spec['pk']} = ?",
                (key,),
            ).fetchone()
        return None if row is None else self._decode_row(dict(row), spec["json"])

    def schema_inventory(self) -> Dict[str, Any]:
        """Compare governed repository column maps with the physical SQLite schema."""

        tables: Dict[str, Any] = {}
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            for record_type, spec in self._TABLES.items():
                object_row = conn.execute(
                    "SELECT type FROM sqlite_master WHERE name = ?",
                    (spec["table"],),
                ).fetchone()
                if object_row is None or str(object_row["type"]) != "table":
                    tables[record_type] = {
                        "table": spec["table"],
                        "status": "migration_required",
                        "missing_columns": list(spec["columns"]),
                        "extra_columns": [],
                    }
                    continue
                physical = {
                    str(row["name"])
                    for row in conn.execute(
                        f"PRAGMA table_info({spec['table']})"
                    ).fetchall()
                }
                declared = set(spec["columns"])
                missing = sorted(declared - physical)
                extra = sorted(physical - declared)
                tables[record_type] = {
                    "table": spec["table"],
                    "status": "current" if not missing else "migration_required",
                    "missing_columns": missing,
                    "extra_columns": extra,
                }
        return {
            "status": (
                "current"
                if all(item["status"] == "current" for item in tables.values())
                else "migration_required"
            ),
            "tables": tables,
        }

    def identity_collision_report(
        self, *, instrument_id: Optional[str] = None, limit: int = 10000
    ) -> Dict[str, Any]:
        """Report records that share a legacy temporal identity.

        This is intentionally read-only and uses the same policy fields as
        ``get_approved_as_of``.  It exposes legacy compression risk without
        rewriting historical rows whose source lineage cannot be reconstructed.
        """
        report: Dict[str, Any] = {"instrument_id": instrument_id, "record_types": {}}
        for record_type in (
            "activities", "operating_facts", "relationships", "exposure_facts", "exposures"
        ):
            policy = get_business_profile_temporal_policy(record_type)
            rows = self.list_records(record_type, instrument_id=instrument_id, limit=limit)
            groups: Dict[tuple[str, ...], list[str]] = {}
            for row in rows:
                values = []
                for field in policy.stable_identity_fields:
                    value = row.get(field)
                    if value is None:
                        value = (row.get("metadata") or {}).get(field)
                    values.append(str(value or ""))
                key = tuple(values)
                record_id = str(
                    row.get("record_id")
                    or row.get("activity_id")
                    or row.get("relationship_id")
                    or row.get("fact_id")
                    or row.get("exposure_id")
                    or "unknown"
                )
                groups.setdefault(key, []).append(record_id)
            collisions = [
                {"identity": list(key), "record_ids": ids}
                for key, ids in groups.items()
                if len(ids) > 1
            ]
            report["record_types"][record_type] = {
                "rows": len(rows),
                "collision_groups": collisions,
                "collision_count": len(collisions),
            }
        return report

    def list_records_page(
        self,
        record_type: str,
        *,
        instrument_id: Optional[str] = None,
        review_status: Optional[str] = None,
        page_size: int = 1000,
        cursor: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Return one deterministic diagnostic page with an explicit cursor."""

        spec = self._TABLES.get(record_type)
        if spec is None:
            raise ValueError(f"unsupported business profile record type: {record_type}")
        clauses: List[str] = []
        params: List[Any] = []
        if instrument_id:
            clauses.append("instrument_id = ?")
            params.append(instrument_id)
        if review_status:
            clauses.append("review_status = ?")
            params.append(review_status)
        cursor_updated_at = str((cursor or {}).get("updated_at") or "").strip()
        cursor_pk = str((cursor or {}).get("primary_key") or "").strip()
        if cursor_updated_at or cursor_pk:
            if not cursor_updated_at or not cursor_pk:
                raise ValueError("diagnostic cursor requires updated_at and primary_key")
            clauses.append(f"(updated_at < ? OR (updated_at = ? AND {spec['pk']} < ?))")
            params.extend([cursor_updated_at, cursor_updated_at, cursor_pk])
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        bounded_size = max(1, min(int(page_size), 10000))
        params.append(bounded_size + 1)
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                f"SELECT * FROM {spec['table']} {where} "
                f"ORDER BY updated_at DESC, {spec['pk']} DESC LIMIT ?",
                params,
            ).fetchall()
        has_more = len(rows) > bounded_size
        selected = rows[:bounded_size]
        records = [self._decode_row(dict(row), spec["json"]) for row in selected]
        next_cursor = None
        if has_more and records:
            last = records[-1]
            next_cursor = {
                "updated_at": str(last.get("updated_at") or ""),
                "primary_key": str(last.get(spec["pk"]) or ""),
            }
        return {"records": records, "next_cursor": next_cursor}

    def get_approved_as_of(
        self,
        record_type: str,
        *,
        instrument_id: str,
        cutoff: str,
        business_regime_id: Optional[str] = None,
        require_null_business_regime: bool = False,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Select latest eligible approved records in indexed SQL."""

        spec = self._TABLES.get(record_type)
        if spec is None:
            raise ValueError(f"unsupported business profile record type: {record_type}")
        policy = get_business_profile_temporal_policy(record_type)
        alias = "r"
        clauses = [f"{alias}.instrument_id = ?", f"{alias}.review_status = 'approved'"]
        params: List[Any] = [instrument_id]
        if "data_available_date" in spec["columns"]:
            clauses.append(f"{alias}.data_available_date <= ?")
            params.append(cutoff)
        if "knowledge_from" in spec["columns"]:
            clauses.append(f"COALESCE({alias}.knowledge_from, {alias}.data_available_date) <= ?")
            clauses.append(f"({alias}.knowledge_to IS NULL OR {alias}.knowledge_to > ?)")
            params.extend([cutoff, cutoff])
        if policy.temporal_class == BusinessProfileTemporalClass.REPORT_FLOW:
            clauses.append(f"{alias}.{policy.observation_period_field} <= ?")
            params.append(cutoff)
            if policy.freshness_days is not None:
                clauses.append(
                    f"julianday(?) - julianday({alias}.{policy.observation_period_field}) "
                    "<= ?"
                )
                params.extend([cutoff, policy.freshness_days])
        elif policy.validity_start_field:
            clauses.append(
                f"({alias}.{policy.validity_start_field} IS NULL "
                f"OR {alias}.{policy.validity_start_field} <= ?)"
            )
            params.append(cutoff)
            clauses.append(
                f"({alias}.{policy.validity_end_field} IS NULL "
                f"OR {alias}.{policy.validity_end_field} > ?)"
            )
            params.append(cutoff)
        if business_regime_id is not None and "business_regime_id" in spec["columns"]:
            clauses.append(f"{alias}.business_regime_id = ?")
            params.append(business_regime_id)
        elif require_null_business_regime and "business_regime_id" in spec["columns"]:
            clauses.append(f"{alias}.business_regime_id IS NULL")
        join = ""
        if record_type != "evidence" and "evidence_id" in spec["columns"]:
            optional_evidence = record_type == "exposure_assumptions"
            join = (
                f"{'LEFT JOIN' if optional_evidence else 'JOIN'} "
                "business_profile_evidence e ON e.evidence_id = r.evidence_id "
                "AND e.instrument_id = r.instrument_id "
            )
            evidence_gate = (
                "e.review_status = 'approved' AND e.data_available_date <= ? "
                "AND e.source_document_id IS NOT NULL "
                "AND e.document_hash IS NOT NULL "
                "AND e.evidence_text_hash IS NOT NULL"
            )
            clauses.append(
                f"(r.evidence_id IS NULL OR ({evidence_gate}))"
                if optional_evidence
                else evidence_gate
            )
            params.append(cutoff)
        supersession_column = get_business_profile_supersession_column(record_type)
        if supersession_column in spec["columns"]:
            clauses.append(
                f"NOT EXISTS (SELECT 1 FROM {spec['table']} successor "
                f"WHERE successor.{supersession_column} = {alias}.{spec['pk']} "
                "AND successor.review_status = 'approved' "
                "AND successor.data_available_date <= ?)"
            )
            params.append(cutoff)
        def identity_expr(field: str) -> str:
            if field in spec["columns"]:
                return f"{alias}.{field}"
            # Lineage fields are stored in metadata_json for schema compatibility.
            return f"json_extract({alias}.metadata_json, '$.{field}')"

        partition_fields = [identity_expr(field) for field in policy.stable_identity_fields]
        if record_type in {"activities", "relationships"}:
            # Source-row and contract lineage live in metadata_json for schema
            # compatibility and must participate in temporal de-duplication.
            partition_fields.extend(
                [
                    "json_extract(r.metadata_json, '$.source_row_key')",
                    "json_extract(r.metadata_json, '$.contract_reference_raw')",
                ]
            )
        partition = ", ".join(partition_fields)
        def output_order_expr(field: str) -> str:
            if field in spec["columns"]:
                return field
            return f"json_extract(metadata_json, '$.{field}')"
        order_fields = []
        if policy.observation_period_field:
            order_fields.append(f"{alias}.{policy.observation_period_field} DESC")
        if "knowledge_from" in spec["columns"]:
            order_fields.append(f"COALESCE({alias}.knowledge_from, '') DESC")
        if "version" in spec["columns"]:
            order_fields.append(f"{alias}.version DESC")
        order_fields.extend([f"{alias}.updated_at DESC", f"{alias}.{spec['pk']} DESC"])
        sql = f"""
            WITH eligible AS (
                SELECT {alias}.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY {partition}
                           ORDER BY {', '.join(order_fields)}
                       ) AS eligibility_rank
                FROM {spec['table']} {alias}
                {join}
                WHERE {' AND '.join(clauses)}
            )
            SELECT * FROM eligible
            WHERE eligibility_rank = 1
            ORDER BY {', '.join(output_order_expr(field) + ' ASC' for field in policy.stable_identity_fields)}
        """
        if limit is not None:
            sql += " LIMIT ?"
            params.append(max(1, min(int(limit), 10000)))
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(sql, params).fetchall()
        output = []
        for row in rows:
            decoded = dict(row)
            decoded.pop("eligibility_rank", None)
            output.append(self._decode_row(decoded, spec["json"]))
        return output

    def get_profile_history(self, instrument_id: str, *, limit: int = 5000) -> Dict[str, Any]:
        return {
            record_type: self.list_records(record_type, instrument_id=instrument_id, limit=limit)
            for record_type in (
                "evidence",
                "events",
                "regimes",
                "segments",
                "operating_facts",
                "activities",
                "value_chain_roles",
                "relationships",
                "exposure_facts",
                "exposure_assumptions",
                "exposures",
            )
        }

    def get_review_queue(
        self,
        *,
        instrument_id: Optional[str] = None,
        record_type: Optional[str] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        record_types = [record_type] if record_type else [
            "evidence", "events", "regimes", "segments", "operating_facts",
            "activities", "value_chain_roles", "relationships", "exposure_facts",
            "exposure_assumptions", "exposures",
        ]
        queue: List[Dict[str, Any]] = []
        for item_type in record_types:
            for row in self.list_records(
                item_type,
                instrument_id=instrument_id,
                review_status="candidate",
                limit=limit,
            ):
                queue.append({"record_type": item_type, **row})
        queue.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        return queue[: max(1, min(int(limit), 1000))]

    def list_exceptions(
        self,
        *,
        instrument_id: Optional[str] = None,
        target_type: Optional[str] = None,
        status: str = "open",
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        clauses = ["status = ?"]
        params: List[Any] = [status]
        if instrument_id:
            clauses.append("instrument_id = ?")
            params.append(instrument_id)
        if target_type:
            clauses.append("target_type = ?")
            params.append(target_type)
        params.append(max(1, min(int(limit), 10000)))
        try:
            with self.storage.get_connection() as conn:
                self.storage._apply_pragmas(conn)
                rows = conn.execute(
                    "SELECT * FROM business_profile_exceptions "
                    f"WHERE {' AND '.join(clauses)} "
                    "ORDER BY updated_at DESC, exception_id DESC LIMIT ?",
                    params,
                ).fetchall()
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return []
            raise
        return [
            self._decode_row(
                dict(row),
                (
                    "reason_codes_json",
                    "evidence_references_json",
                    "ranked_choices_json",
                    "metadata_json",
                ),
            )
            for row in rows
        ]

    @staticmethod
    def _validate_interval(start_value: Any, end_value: Any, label: str) -> None:
        start = _date_key(start_value)
        end = _date_key(end_value)
        if start and end and end < start:
            raise ValueError(f"{label}_to cannot be earlier than {label}_from")

    @staticmethod
    def _decode_row(row: Dict[str, Any], json_columns: Sequence[str]) -> Dict[str, Any]:
        for column in json_columns:
            value = row.pop(column, None)
            output_key = column[:-5] if column.endswith("_json") else column
            row[output_key] = _json_loads(value, {})
        return row


class BusinessProfileResolver:
    """Build an auditable point-in-time business profile without remote access."""

    def __init__(
        self,
        repository: BusinessProfileRepository,
        futures_storage: Any = None,
        special_commodity_storage: Any = None,
    ):
        self.repository = repository
        self.futures_storage = futures_storage
        self.special_commodity_storage = special_commodity_storage

    def resolve(
        self,
        instrument_id: str,
        *,
        as_of_date: str,
        industry_membership: Optional[Dict[str, Any]] = None,
        include_candidates: bool = True,
    ) -> Dict[str, Any]:
        cutoff = _date_key(as_of_date) or get_shanghai_time().date().isoformat()
        try:
            history = self._load_resolver_history(
                instrument_id,
                cutoff,
                record_types=("evidence", "events", "regimes"),
            )
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc) and "no such column" not in str(exc):
                raise
            payload = build_empty_business_profile_context(
                instrument_id,
                as_of_date=cutoff,
                warning="business_profile_storage_migration_required",
            )
            payload["readiness"]["storage_status"] = "migration_required"
            payload["readiness"]["storage_error"] = str(exc)
            payload["readiness"]["temporal_coverage"] = {}
            return payload
        evidence = {item["evidence_id"]: item for item in history["evidence"]}
        lifecycle = self._resolve_lifecycle(
            history,
            evidence,
            cutoff,
            include_candidates=include_candidates,
        )
        active_regime = lifecycle["active_regime"]
        active_regime_id = (
            str(active_regime.get("regime_id"))
            if isinstance(active_regime, dict)
            else None
        )
        has_governed_regimes = bool(lifecycle["approved_regimes"])
        history.update(
            self._load_resolver_history(
                instrument_id,
                cutoff,
                record_types=(
                    "segments",
                    "operating_facts",
                    "activities",
                    "value_chain_roles",
                    "relationships",
                    "exposure_facts",
                    "exposures",
                ),
                business_regime_id=active_regime_id,
                require_null_business_regime=not has_governed_regimes,
            )
        )
        approved: Dict[str, List[Dict[str, Any]]] = {}
        candidates: Dict[str, List[Dict[str, Any]]] = {}
        warnings: List[str] = list(lifecycle["warnings"])
        for fact_type in (
            "segments",
            "operating_facts",
            "activities",
            "value_chain_roles",
            "relationships",
            "exposure_facts",
            "exposures",
        ):
            approved[fact_type] = []
            candidates[fact_type] = []
            for fact in history[fact_type]:
                eligible_date = self._is_date_eligible(fact, cutoff, fact_type)
                evidence_row = evidence.get(fact.get("evidence_id"))
                evidence_valid = self._evidence_is_valid(evidence_row, cutoff)
                regime_eligible = self._fact_regime_is_eligible(
                    fact,
                    active_regime_id=active_regime_id,
                    has_governed_regimes=has_governed_regimes,
                )
                if (
                    fact.get("review_status") == "approved"
                    and eligible_date
                    and evidence_valid
                    and regime_eligible
                ):
                    approved[fact_type].append(fact)
                elif include_candidates:
                    diagnostic = dict(fact)
                    diagnostic["eligibility"] = {
                        "date_eligible": eligible_date,
                        "evidence_valid": evidence_valid,
                        "regime_eligible": regime_eligible,
                        "active_regime_id": active_regime_id,
                        "review_status": fact.get("review_status"),
                    }
                    candidates[fact_type].append(diagnostic)
                if fact.get("review_status") == "approved" and not evidence_valid:
                    warnings.append(
                        f"invalid_evidence:{fact_type}:{self._record_id(fact)}"
                    )
                if (
                    fact.get("review_status") == "approved"
                    and eligible_date
                    and evidence_valid
                    and not regime_eligible
                ):
                    warnings.append(
                        f"inactive_business_regime:{fact_type}:{self._record_id(fact)}"
                    )

        approved["activities"] = select_current_business_profile_activities(
            approved["activities"]
        )

        temporal_coverage = self._temporal_coverage(
            approved=approved,
            candidates=candidates,
            cutoff=cutoff,
        )
        exceptions = self.repository.list_exceptions(
            instrument_id=instrument_id,
            status="open",
        ) if include_candidates else []

        industry_mappings, industry_scope = self._load_industry_mappings(industry_membership, cutoff)
        executable_company, mapping_gaps = self._company_executable_mappings(approved["exposures"])
        selected_mappings = self._merge_mappings(executable_company, industry_mappings)
        scores = self._model_scores(industry_membership, industry_mappings, approved)
        conflicts = self._detect_conflicts(executable_company, industry_mappings)
        recommendation = self._recommend_model(scores, conflicts, bool(executable_company))
        if not any(approved.values()):
            warnings.append("insufficient_company_evidence")
        warnings.extend(mapping_gaps)
        profile_payload = {
            "schema_version": BUSINESS_PROFILE_SCHEMA_VERSION,
            "instrument_id": instrument_id,
            "data_available_cutoff": cutoff,
            "industry_default_profile": {
                "industry_membership": industry_membership,
                "mapping_scope_id": industry_scope,
                "exposure_mappings": industry_mappings,
            },
            "company_specific_profile": {
                "business_regime": active_regime,
                "segments": approved["segments"],
                "operating_facts": approved["operating_facts"],
                "activities": approved["activities"],
                "value_chain_roles": approved["value_chain_roles"],
                "supply_chain_relationships": approved["relationships"],
                "commodity_exposure_facts": approved["exposure_facts"],
                "commodity_exposures": approved["exposures"],
            },
            "measurement_contract": _build_business_profile_measurement_contract(
                approved["operating_facts"]
            ),
            "segment_profiles": approved["segments"],
            "approved_exposures": approved["exposures"],
            "candidate_exposures": candidates["exposures"] if include_candidates else [],
            "candidate_facts": candidates if include_candidates else {},
            "exceptions": exceptions,
            "profile_lifecycle": {
                key: value for key, value in lifecycle.items() if key != "warnings"
            },
            "executable_exposure_mappings": selected_mappings,
            "model_scores": scores,
            "model_recommendation": recommendation,
            "conflicts": conflicts,
            "warnings": sorted(set(warnings)),
        }
        profile_payload["profile_version"] = _stable_hash(profile_payload)[:24]
        profile_payload["lineage_hash"] = _stable_hash(
            {
                "facts": [item.get("lineage_hash") for values in approved.values() for item in values],
                "industry_scope": industry_scope,
                "active_regime": active_regime_id,
                "profile_events": [
                    item.get("lineage_hash") for item in lifecycle["approved_events"]
                ],
                "mappings": [item.get("mapping_id") for item in selected_mappings],
            }
        )
        approved_company_fact_count = sum(len(values) for values in approved.values())
        approved_exposure_count = len(approved["exposures"])
        profile_status, market_link_status, unresolved_exposure_ids = (
            _derive_profile_and_market_link_status(
                approved_company_fact_count=approved_company_fact_count,
                approved_exposures=approved["exposures"],
                executable_company_mappings=executable_company,
            )
        )
        executable_company_count = len(executable_company)
        # Profile readiness answers whether the company facts are usable.  A
        # missing market series is optional downstream enrichment and must not
        # hide otherwise approved business, supply-chain, or exposure facts.
        profile_payload["status"] = profile_status
        profile_payload["market_link_status"] = market_link_status
        profile_payload["readiness"] = {
            "status": profile_payload["status"],
            "storage_status": "ready",
            "temporal_coverage": temporal_coverage,
            "approved_company_fact_count": approved_company_fact_count,
            "approved_company_exposure_count": approved_exposure_count,
            "active_business_regime_id": active_regime_id,
            "approved_profile_event_count": len(lifecycle["approved_events"]),
            "industry_mapping_count": len(industry_mappings),
            "executable_mapping_count": len(selected_mappings),
            "market_link": {
                "status": market_link_status,
                "approved_exposure_count": approved_exposure_count,
                "executable_mapping_count": executable_company_count,
                "unresolved_exposure_ids": unresolved_exposure_ids,
            },
            "input_gaps": sorted(set(
                mapping_gaps
                + lifecycle["blockers"]
                + (["company_business_profile_missing"] if not any(approved.values()) else [])
            )),
            "open_exception_count": len(exceptions),
            "exception_tier_counts": {
                tier: sum(item.get("tier") == tier for item in exceptions)
                for tier in ("machine_rework", "quick_review", "deep_review")
            },
        }
        return profile_payload

    @staticmethod
    def _temporal_coverage(
        *,
        approved: Dict[str, List[Dict[str, Any]]],
        candidates: Dict[str, List[Dict[str, Any]]],
        cutoff: str,
    ) -> Dict[str, Dict[str, Any]]:
        coverage: Dict[str, Dict[str, Any]] = {}
        for record_type in (
            "segments",
            "operating_facts",
            "value_chain_roles",
            "exposures",
        ):
            policy = get_business_profile_temporal_policy(record_type)
            diagnostics = candidates.get(record_type, [])
            excluded_approved = [
                item for item in diagnostics if item.get("review_status") == "approved"
            ]
            held = [item for item in diagnostics if item.get("review_status") == "held"]
            if approved.get(record_type):
                status = "current"
            elif (
                policy.temporal_class == BusinessProfileTemporalClass.REPORT_FLOW
                and excluded_approved
            ):
                status = "stale_flow_coverage"
            elif policy.temporal_class in {
                BusinessProfileTemporalClass.POINT_IN_TIME_STATE,
                BusinessProfileTemporalClass.PERSISTENT_RELATIONSHIP,
            }:
                status = "missing_current_state"
            else:
                status = "missing"
            coverage[record_type] = {
                "status": status,
                "temporal_class": policy.temporal_class.value,
                "freshness_days": policy.freshness_days,
                "approved_current_count": len(approved.get(record_type, [])),
                "excluded_approved_count": len(excluded_approved),
                "unresolved_exception_count": len(held),
                "knowledge_cutoff": cutoff,
            }
        return coverage

    @staticmethod
    def _record_id(record: Dict[str, Any]) -> str:
        return str(
            record.get("record_id")
            or record.get("activity_id")
            or record.get("relationship_id")
            or record.get("fact_id")
            or record.get("assumption_id")
            or record.get("exposure_id")
            or "unknown"
        )

    @staticmethod
    def _is_date_eligible(record: Dict[str, Any], cutoff: str, record_type: str) -> bool:
        available = _date_key(record.get("data_available_date"))
        if not available or available > cutoff:
            return False
        knowledge_from = _date_key(record.get("knowledge_from")) or available
        knowledge_to = _date_key(record.get("knowledge_to"))
        if knowledge_from > cutoff or (knowledge_to and cutoff >= knowledge_to):
            return False
        policy = get_business_profile_temporal_policy(record_type)
        if policy.temporal_class == BusinessProfileTemporalClass.REPORT_FLOW:
            observed = _date_key(record.get(policy.observation_period_field or ""))
            if not observed or observed > cutoff:
                return False
            if policy.freshness_days is None:
                return True
            return (date.fromisoformat(cutoff) - date.fromisoformat(observed)).days <= (
                policy.freshness_days
            )
        start_field = "effective_from" if record_type == "exposures" else "valid_from"
        end_field = "effective_to" if record_type == "exposures" else "valid_to"
        start = _date_key(record.get(start_field))
        end = _date_key(record.get(end_field))
        # Validity intervals are half-open: [valid_from, valid_to).
        return (not start or start <= cutoff) and (not end or cutoff < end)

    def _load_resolver_history(
        self,
        instrument_id: str,
        cutoff: str,
        *,
        record_types: Sequence[str],
        business_regime_id: Optional[str] = None,
        require_null_business_regime: bool = False,
    ) -> Dict[str, List[Dict[str, Any]]]:
        history: Dict[str, List[Dict[str, Any]]] = {}
        for record_type in record_types:
            approved = self.repository.get_approved_as_of(
                record_type,
                instrument_id=instrument_id,
                cutoff=cutoff,
                business_regime_id=business_regime_id,
                require_null_business_regime=require_null_business_regime,
            )
            approved_ids = {
                str(item[BusinessProfileRepository._TABLES[record_type]["pk"]])
                for item in approved
            }
            excluded_approved_diagnostics = [
                item
                for item in self.repository.list_records(
                    record_type,
                    instrument_id=instrument_id,
                    review_status="approved",
                    limit=10000,
                )
                if str(item[BusinessProfileRepository._TABLES[record_type]["pk"]])
                not in approved_ids
            ]
            candidates = self.repository.list_records(
                record_type,
                instrument_id=instrument_id,
                review_status="candidate",
                limit=10000,
            )
            held = self.repository.list_records(
                record_type,
                instrument_id=instrument_id,
                review_status="held",
                limit=10000,
            )
            history[record_type] = (
                approved + excluded_approved_diagnostics + candidates + held
            )
        return history

    @staticmethod
    def _fact_regime_is_eligible(
        fact: Dict[str, Any],
        *,
        active_regime_id: Optional[str],
        has_governed_regimes: bool,
    ) -> bool:
        regime_id = str(fact.get("business_regime_id") or "").strip() or None
        if not has_governed_regimes:
            return regime_id is None
        return bool(active_regime_id and regime_id == active_regime_id)

    def _resolve_lifecycle(
        self,
        history: Dict[str, List[Dict[str, Any]]],
        evidence: Dict[str, Dict[str, Any]],
        cutoff: str,
        *,
        include_candidates: bool,
    ) -> Dict[str, Any]:
        approved_regimes: List[Dict[str, Any]] = []
        active_regimes: List[Dict[str, Any]] = []
        candidate_regimes: List[Dict[str, Any]] = []
        approved_events: List[Dict[str, Any]] = []
        candidate_events: List[Dict[str, Any]] = []
        warnings: List[str] = []

        for regime in history.get("regimes", []):
            evidence_valid = self._evidence_is_valid(
                evidence.get(regime.get("evidence_id")), cutoff
            )
            knowledge_eligible = self._knowledge_is_eligible(regime, cutoff)
            business_eligible = self._business_interval_is_eligible(regime, cutoff)
            if (
                regime.get("review_status") == "approved"
                and evidence_valid
                and knowledge_eligible
            ):
                approved_regimes.append(regime)
                if business_eligible:
                    active_regimes.append(regime)
            elif include_candidates:
                diagnostic = dict(regime)
                diagnostic["eligibility"] = {
                    "evidence_valid": evidence_valid,
                    "knowledge_eligible": knowledge_eligible,
                    "business_eligible": business_eligible,
                    "review_status": regime.get("review_status"),
                }
                candidate_regimes.append(diagnostic)

        active_regimes.sort(
            key=lambda item: (
                _date_key(item.get("valid_from")) or "",
                _date_key(item.get("knowledge_from")) or "",
                int(item.get("version") or 0),
            )
        )
        active_regime = active_regimes[0] if len(active_regimes) == 1 else None
        blockers: List[str] = []
        if len(active_regimes) > 1:
            warnings.append("overlapping_active_business_regimes")
            blockers.append("overlapping_active_business_regimes")

        for event in history.get("events", []):
            evidence_valid = self._evidence_is_valid(
                evidence.get(event.get("evidence_id")), cutoff
            )
            available = _date_key(event.get("data_available_date"))
            known = bool(available and available <= cutoff)
            event_date = _date_key(event.get("event_date"))
            effective = not event_date or event_date <= cutoff
            if (
                event.get("review_status") == "approved"
                and evidence_valid
                and known
                and effective
            ):
                approved_events.append(event)
            elif include_candidates:
                diagnostic = dict(event)
                diagnostic["eligibility"] = {
                    "evidence_valid": evidence_valid,
                    "knowledge_eligible": known,
                    "business_eligible": effective,
                    "review_status": event.get("review_status"),
                }
                candidate_events.append(diagnostic)
            if (
                event.get("review_status") == "candidate"
                and known
                and str(event.get("event_type") or "") in MATERIAL_PROFILE_EVENT_TYPES
                and str(event.get("materiality") or "").lower() in {"high", "material"}
            ):
                warnings.append(
                    f"material_profile_change_pending_review:{event.get('event_id')}"
                )

        return {
            "active_regime": active_regime,
            "approved_regimes": approved_regimes,
            "approved_events": approved_events,
            "candidate_regimes": candidate_regimes,
            "candidate_events": candidate_events,
            "blockers": blockers,
            "warnings": warnings,
        }

    @staticmethod
    def _knowledge_is_eligible(record: Dict[str, Any], cutoff: str) -> bool:
        available = _date_key(record.get("data_available_date"))
        start = _date_key(record.get("knowledge_from")) or available
        end = _date_key(record.get("knowledge_to"))
        return bool(
            available
            and available <= cutoff
            and start
            and start <= cutoff
            and (not end or cutoff < end)
        )

    @staticmethod
    def _business_interval_is_eligible(record: Dict[str, Any], cutoff: str) -> bool:
        start = _date_key(record.get("valid_from"))
        end = _date_key(record.get("valid_to"))
        return (not start or start <= cutoff) and (not end or cutoff < end)

    @staticmethod
    def _evidence_is_valid(evidence: Optional[Dict[str, Any]], cutoff: str) -> bool:
        if not evidence or evidence.get("review_status") != "approved":
            return False
        available = _date_key(evidence.get("data_available_date"))
        return bool(
            available
            and available <= cutoff
            and evidence.get("source_document_id")
            and evidence.get("document_hash")
            and evidence.get("evidence_text_hash")
        )

    def _load_industry_mappings(
        self,
        membership: Optional[Dict[str, Any]],
        cutoff: str,
    ) -> tuple[List[Dict[str, Any]], Optional[str]]:
        if self.futures_storage is None or not isinstance(membership, dict):
            return [], None
        fields = (
            "industry_code", "sw_l3_code", "sw_l2_code", "sw_l1_code",
            "best_taxonomy_industry_code", "mapped_industry_code", "industry_name",
            "sw_l3_name", "sw_l2_name", "sw_l1_name",
        )
        seen = set()
        for field in fields:
            scope_id = str(membership.get(field) or "").strip()
            if not scope_id or scope_id in seen:
                continue
            seen.add(scope_id)
            rows = self.futures_storage.get_exposure_mappings(scope_type="industry", scope_id=scope_id)
            eligible = [row for row in rows if self._mapping_date_eligible(row, cutoff)]
            if eligible:
                return eligible, scope_id
        return [], None

    @staticmethod
    def _mapping_date_eligible(mapping: Dict[str, Any], cutoff: str) -> bool:
        start = _date_key(mapping.get("valid_from"))
        end = _date_key(mapping.get("valid_to"))
        return (not start or start <= cutoff) and (not end or cutoff < end)

    def _company_executable_mappings(
        self,
        exposures: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], List[str]]:
        mappings: List[Dict[str, Any]] = []
        gaps: List[str] = []
        for exposure in exposures:
            series_id = str(exposure.get("price_series_id") or "").strip() or None
            spread_id = str(exposure.get("spread_definition_id") or "").strip() or None
            market_data_family = None
            if series_id:
                series, market_data_family = self._resolve_market_series(series_id)
                if not series or not series.get("active", False):
                    gaps.append(f"inactive_or_missing_series:{series_id}")
                    series_id = None
                    market_data_family = None
            if not series_id and not spread_id:
                gaps.append(f"exposure_market_series_missing:{exposure.get('exposure_id')}")
                continue
            role = str(exposure.get("exposure_role") or "revenue")
            is_cost = role in {"feedstock_cost", "energy_cost"}
            direction = str(exposure.get("direction") or "").strip() or None
            if direction is None:
                gaps.append(
                    f"exposure_direction_missing:{exposure.get('exposure_id')}"
                )
                continue
            mappings.append(
                {
                    "mapping_id": f"business-profile:{exposure['exposure_id']}",
                    "scope_type": "instrument",
                    "scope_id": exposure["instrument_id"],
                    "product_name": exposure.get("commodity_id") or exposure["exposure_id"],
                    "commodity_id": exposure.get("commodity_id"),
                    "exposure_role": role,
                    "revenue_series_id": None if is_cost else series_id,
                    "cost_series_ids": [series_id] if is_cost and series_id else [],
                    "spread_ids": [spread_id] if spread_id else [],
                    "direction": direction,
                    "transmission_strength": exposure.get("materiality"),
                    "lag_days": exposure.get("lag_days"),
                    "confidence": exposure.get("confidence"),
                    "source": "approved_company_business_profile",
                    "market_data_family": market_data_family,
                    "source_exposure_id": exposure.get("exposure_id"),
                    "valid_from": exposure.get("effective_from"),
                    "valid_to": exposure.get("effective_to"),
                    "lineage_hash": exposure.get("lineage_hash"),
                }
            )
        return mappings, gaps

    def _resolve_market_series(
        self,
        series_id: str,
    ) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
        if self.futures_storage is not None:
            series = self.futures_storage.get_series(series_id)
            if series is not None:
                return series, "futures"
        if self.special_commodity_storage is not None:
            series = self.special_commodity_storage.get_series(series_id)
            if series is not None:
                return series, "special_commodity"
        return None, None

    @staticmethod
    def _merge_mappings(
        company: List[Dict[str, Any]], industry: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        selected = list(company)
        def key(item: Mapping[str, Any]) -> tuple[str, str, str, str]:
            return (
                str(item.get("commodity_id") or item.get("product_name") or "").lower(),
                str(item.get("exposure_role") or "revenue"),
                str(item.get("revenue_series_id") or ""),
                ",".join(sorted(str(value) for value in (item.get("spread_ids") or []))),
            )

        company_keys = {key(item) for item in company}
        for mapping in industry:
            if key(mapping) not in company_keys:
                selected.append(mapping)
        return selected

    @staticmethod
    def _detect_conflicts(
        company: List[Dict[str, Any]], industry: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        conflicts: List[Dict[str, Any]] = []
        industry_by_product = {
            (
                str(item.get("commodity_id") or item.get("product_name") or "").lower(),
                str(item.get("exposure_role") or "revenue"),
            ): item
            for item in industry
        }
        for item in company:
            default = industry_by_product.get(
                (
                    str(item.get("commodity_id") or item.get("product_name") or "").lower(),
                    str(item.get("exposure_role") or "revenue"),
                )
            )
            if default and default.get("direction") != item.get("direction"):
                conflicts.append(
                    {
                        "type": "exposure_direction_conflict",
                        "product_name": item.get("product_name"),
                        "company_direction": item.get("direction"),
                        "industry_direction": default.get("direction"),
                    }
                )
        return conflicts

    @staticmethod
    def _model_scores(
        membership: Optional[Dict[str, Any]],
        industry_mappings: List[Dict[str, Any]],
        approved: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        authoritative = bool(membership and membership.get("mapping_status") == "authoritative")
        industry_score = min(1.0, (0.6 if authoritative else 0.25) + (0.3 if industry_mappings else 0.0))
        exposure_count = len(approved["exposures"])
        company_score = min(
            1.0,
            (0.25 if approved["segments"] else 0.0)
            + (0.2 if approved["value_chain_roles"] else 0.0)
            + min(0.4, exposure_count * 0.2)
            + (0.15 if approved["operating_facts"] else 0.0),
        )
        return {
            "score_version": BUSINESS_PROFILE_SCORE_VERSION,
            "industry_model_score": round(industry_score, 4),
            "company_model_score": round(company_score, 4),
            "components": {
                "authoritative_industry": authoritative,
                "industry_mapping_count": len(industry_mappings),
                "approved_segment_count": len(approved["segments"]),
                "approved_role_count": len(approved["value_chain_roles"]),
                "approved_exposure_count": exposure_count,
                "approved_operating_fact_count": len(approved["operating_facts"]),
            },
        }

    @staticmethod
    def _recommend_model(
        scores: Dict[str, Any], conflicts: List[Dict[str, Any]], has_company_exposure: bool
    ) -> str:
        industry = float(scores["industry_model_score"])
        company = float(scores["company_model_score"])
        if not has_company_exposure:
            return "industry_default"
        if conflicts or abs(company - industry) <= 0.15:
            return "dual_model"
        return "company_specific" if company > industry else "industry_default"
