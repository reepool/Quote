"""Point-in-time governance for company business profiles and commodity exposure."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Iterable, List, Optional, Sequence

from utils.date_utils import get_shanghai_time


BUSINESS_PROFILE_SCHEMA_VERSION = "company_business_profile.v2"
BUSINESS_PROFILE_SCORE_VERSION = "business_profile_model_score.v1"
REVIEW_STATUSES = {"candidate", "approved", "rejected", "superseded"}
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
        "instrument_id": instrument_id,
        "data_available_cutoff": _date_key(as_of_date) or str(as_of_date),
        "industry_default_profile": {},
        "company_specific_profile": {
            "business_regime": None,
            "segments": [],
            "operating_facts": [],
            "value_chain_roles": [],
            "commodity_exposures": [],
        },
        "segment_profiles": [],
        "approved_exposures": [],
        "candidate_exposures": [],
        "candidate_facts": {},
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
                "revenue", "revenue_share", "segment_profit", "segment_assets",
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
                "supersedes_exposure_id", "version", "lineage_hash", "metadata_json",
                "created_at", "updated_at",
            ),
            "json": {"metadata_json"},
        },
    }

    def __init__(self, storage: Any):
        self.storage = storage

    def upsert(self, record_type: str, record: Dict[str, Any]) -> str:
        """Idempotently upsert one governed record by its stable primary key."""
        spec = self._TABLES.get(record_type)
        if spec is None:
            raise ValueError(f"unsupported business profile record type: {record_type}")
        payload = dict(record)
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
        if record_type == "exposures":
            payload.setdefault("lag_days", 0)
        if "knowledge_from" in spec["columns"]:
            payload.setdefault("knowledge_from", payload.get("data_available_date"))
            if not payload.get("knowledge_from"):
                raise ValueError("knowledge_from or data_available_date is required")
            self._validate_interval(
                payload.get("knowledge_from"),
                payload.get("knowledge_to"),
                "knowledge",
            )
        if record_type == "regimes":
            self._validate_interval(
                payload.get("valid_from"), payload.get("valid_to"), "valid"
            )
        now = get_shanghai_time().isoformat()
        payload.setdefault("created_at", now)
        payload["updated_at"] = now
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
        updates = [f"{column}=excluded.{column}" for column in columns if column not in {pk, "created_at"}]
        sql = f"""
            INSERT INTO {spec['table']} ({', '.join(columns)})
            VALUES ({', '.join('?' for _ in columns)})
            ON CONFLICT({pk}) DO UPDATE SET {', '.join(updates)}
        """
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute(sql, values)
            conn.commit()
        return str(payload[pk])

    def upsert_many(self, record_type: str, records: Iterable[Dict[str, Any]]) -> int:
        count = 0
        for record in records:
            self.upsert(record_type, record)
            count += 1
        return count

    def list_records(
        self,
        record_type: str,
        *,
        instrument_id: Optional[str] = None,
        review_status: Optional[str] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
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
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(1, min(int(limit), 10000)))
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                f"SELECT * FROM {spec['table']} {where} ORDER BY updated_at DESC LIMIT ?",
                params,
            ).fetchall()
        return [self._decode_row(dict(row), spec["json"]) for row in rows]

    def get_profile_history(self, instrument_id: str, *, limit: int = 5000) -> Dict[str, Any]:
        return {
            record_type: self.list_records(record_type, instrument_id=instrument_id, limit=limit)
            for record_type in (
                "evidence",
                "events",
                "regimes",
                "segments",
                "operating_facts",
                "value_chain_roles",
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
            "value_chain_roles", "exposures",
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

    def __init__(self, repository: BusinessProfileRepository, futures_storage: Any = None):
        self.repository = repository
        self.futures_storage = futures_storage

    def resolve(
        self,
        instrument_id: str,
        *,
        as_of_date: str,
        industry_membership: Optional[Dict[str, Any]] = None,
        include_candidates: bool = True,
    ) -> Dict[str, Any]:
        cutoff = _date_key(as_of_date) or get_shanghai_time().date().isoformat()
        history = self.repository.get_profile_history(instrument_id)
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
        approved: Dict[str, List[Dict[str, Any]]] = {}
        candidates: Dict[str, List[Dict[str, Any]]] = {}
        warnings: List[str] = list(lifecycle["warnings"])
        for fact_type in ("segments", "operating_facts", "value_chain_roles", "exposures"):
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
                "value_chain_roles": approved["value_chain_roles"],
                "commodity_exposures": approved["exposures"],
            },
            "segment_profiles": approved["segments"],
            "approved_exposures": approved["exposures"],
            "candidate_exposures": candidates["exposures"] if include_candidates else [],
            "candidate_facts": candidates if include_candidates else {},
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
        profile_payload["status"] = (
            "ready" if executable_company else "industry_fallback" if industry_mappings else "not_ready"
        )
        profile_payload["readiness"] = {
            "status": profile_payload["status"],
            "approved_company_fact_count": sum(len(values) for values in approved.values()),
            "approved_company_exposure_count": len(approved["exposures"]),
            "active_business_regime_id": active_regime_id,
            "approved_profile_event_count": len(lifecycle["approved_events"]),
            "industry_mapping_count": len(industry_mappings),
            "executable_mapping_count": len(selected_mappings),
            "input_gaps": sorted(set(
                mapping_gaps
                + lifecycle["blockers"]
                + (["company_business_profile_missing"] if not any(approved.values()) else [])
            )),
        }
        return profile_payload

    @staticmethod
    def _record_id(record: Dict[str, Any]) -> str:
        return str(record.get("record_id") or record.get("exposure_id") or "unknown")

    @staticmethod
    def _is_date_eligible(record: Dict[str, Any], cutoff: str, record_type: str) -> bool:
        available = _date_key(record.get("data_available_date"))
        if not available or available > cutoff:
            return False
        knowledge_from = _date_key(record.get("knowledge_from")) or available
        knowledge_to = _date_key(record.get("knowledge_to"))
        if knowledge_from > cutoff or (knowledge_to and cutoff >= knowledge_to):
            return False
        start_field = "effective_from" if record_type == "exposures" else "valid_from"
        end_field = "effective_to" if record_type == "exposures" else "valid_to"
        start = _date_key(record.get(start_field))
        end = _date_key(record.get(end_field))
        return (not start or start <= cutoff) and (not end or end >= cutoff)

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
        return (not start or start <= cutoff) and (not end or end >= cutoff)

    def _company_executable_mappings(
        self,
        exposures: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], List[str]]:
        mappings: List[Dict[str, Any]] = []
        gaps: List[str] = []
        for exposure in exposures:
            series_id = str(exposure.get("price_series_id") or "").strip() or None
            spread_id = str(exposure.get("spread_definition_id") or "").strip() or None
            if series_id and self.futures_storage is not None:
                series = self.futures_storage.get_series(series_id)
                if not series or not series.get("active", False):
                    gaps.append(f"inactive_or_missing_series:{series_id}")
                    series_id = None
            if not series_id and not spread_id:
                gaps.append(f"exposure_market_series_missing:{exposure.get('exposure_id')}")
                continue
            role = str(exposure.get("exposure_role") or "revenue")
            is_cost = role in {"feedstock_cost", "energy_cost"}
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
                    "direction": exposure.get("direction") or ("negative" if is_cost else "positive"),
                    "transmission_strength": exposure.get("materiality") or "medium",
                    "lag_days": int(exposure.get("lag_days") or 0),
                    "confidence": exposure.get("confidence"),
                    "source": "approved_company_business_profile",
                    "valid_from": exposure.get("effective_from"),
                    "valid_to": exposure.get("effective_to"),
                    "lineage_hash": exposure.get("lineage_hash"),
                }
            )
        return mappings, gaps

    @staticmethod
    def _merge_mappings(
        company: List[Dict[str, Any]], industry: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        selected = list(company)
        company_keys = {
            (str(item.get("product_name") or "").lower(), str(item.get("exposure_role") or "revenue"))
            for item in company
        }
        for mapping in industry:
            key = (str(mapping.get("product_name") or "").lower(), "revenue")
            if key not in company_keys:
                selected.append(mapping)
        return selected

    @staticmethod
    def _detect_conflicts(
        company: List[Dict[str, Any]], industry: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        conflicts: List[Dict[str, Any]] = []
        industry_by_product = {
            str(item.get("product_name") or "").lower(): item for item in industry
        }
        for item in company:
            default = industry_by_product.get(str(item.get("product_name") or "").lower())
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
