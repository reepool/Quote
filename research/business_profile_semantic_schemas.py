"""Closed JSON schemas for business-profile semantic production artifacts."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping

from jsonschema import Draft202012Validator


BUSINESS_PROFILE_SEMANTIC_SCHEMA_SET_VERSION = "business_profile_semantic_schemas.v2"

_HASH = {"type": "string", "pattern": "^[0-9a-f]{64}$"}
_DATE = {"type": "string", "format": "date"}
_OPTIONAL_DATE = {"oneOf": [_DATE, {"type": "null"}]}


def _closed_object(
    properties: Mapping[str, Any],
    required: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": dict(properties),
        "required": list(required),
        "additionalProperties": False,
    }


SELECTED_SECTION_BUNDLE_SCHEMA = _closed_object(
    {
        "schema_version": {"const": "business_profile_selected_section_bundle.v1"},
        "bundle_id": {"type": "string", "minLength": 1},
        "instrument_id": {"type": "string", "minLength": 1},
        "source_document_id": {"type": "string", "minLength": 1},
        "document_hash": _HASH,
        "field_family": {"type": "string", "minLength": 1},
        "selector_version": {"type": "string", "minLength": 1},
        "page_budget": {
            "type": "object",
            "properties": {
                "effective_max_pages": {"type": "integer", "minimum": 1},
                "chapter_page_count": {"type": "integer", "minimum": 1},
                "budget_reason": {"type": "string", "minLength": 1},
            },
            "required": ("effective_max_pages", "chapter_page_count", "budget_reason"),
            "additionalProperties": False,
        },
        "window_index": {"type": "integer", "minimum": 0},
        "window_count": {"type": "integer", "minimum": 1},
        "section_ids": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "minItems": 1,
            "uniqueItems": True,
        },
        "page_ranges": {
            "type": "array",
            "items": _closed_object(
                {
                    "start_page": {"type": "integer", "minimum": 1},
                    "end_page": {"type": "integer", "minimum": 1},
                },
                ("start_page", "end_page"),
            ),
            "minItems": 1,
        },
        "section_hash": _HASH,
        "quality": {"enum": ["native", "governed_ocr", "low_text", "unsupported"]},
        "selector_reasons": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "minItems": 1,
            "uniqueItems": True,
        },
    },
    (
        "schema_version",
        "bundle_id",
        "instrument_id",
        "source_document_id",
        "document_hash",
        "field_family",
        "selector_version",
        "section_ids",
        "page_ranges",
        "section_hash",
        "quality",
        "selector_reasons",
    ),
)

EVIDENCE_SPAN_SCHEMA = _closed_object(
    {
        "evidence_span_id": {"type": "string", "pattern": "^span-[0-9a-f]{24}$"},
        "page_number": {"type": "integer", "minimum": 1},
        "section_id": {"type": "string", "minLength": 1},
        "quote": {"type": "string", "minLength": 1},
        "normalized_start": {"type": "integer", "minimum": 0},
        "normalized_end": {"type": "integer", "minimum": 1},
        "quote_hash": _HASH,
        "section_hash": _HASH,
    },
    (
        "evidence_span_id",
        "page_number",
        "section_id",
        "quote",
        "normalized_start",
        "normalized_end",
        "quote_hash",
        "section_hash",
    ),
)

EXACT_EVIDENCE_SCHEMA = _closed_object(
    {
        "source_document_id": {"type": "string", "minLength": 1},
        "page_number": {"type": "integer", "minimum": 1},
        "section_id": {"type": "string", "minLength": 1},
        "quote": {"type": "string", "minLength": 1},
        "normalized_start": {"type": "integer", "minimum": 0},
        "normalized_end": {"type": "integer", "minimum": 1},
        "quote_hash": _HASH,
        "section_hash": _HASH,
        "evidence_spans": {
            "type": "array",
            "items": EVIDENCE_SPAN_SCHEMA,
            "minItems": 1,
            "uniqueItems": True,
        },
        "composite": {"type": "boolean"},
        "composite_quote": {"type": "string", "minLength": 1},
        "composite_quote_hash": _HASH,
    },
    (
        "source_document_id",
        "page_number",
        "section_id",
        "quote",
        "normalized_start",
        "normalized_end",
        "quote_hash",
        "section_hash",
    ),
)

ATOMIC_ACTIVITY_SCHEMA = _closed_object(
    {
        "schema_version": {"const": "business_profile_atomic_activity.v1"},
        "activity_id": {"type": "string", "minLength": 1},
        "instrument_id": {"type": "string", "minLength": 1},
        "subject_scope": {"enum": ["issuer", "consolidated_group", "named_subsidiary"]},
        "action": {
            "enum": [
                "extracts",
                "cultivates",
                "produces",
                "processes",
                "purchases",
                "consumes",
                "sells",
                "transports",
                "stores",
                "trades",
                "hedges",
            ]
        },
        "object_raw": {"type": "string", "minLength": 1},
        "object_id": {"type": ["string", "null"]},
        "segment_id": {"type": ["string", "null"]},
        "report_period": _DATE,
        "value": {"type": ["number", "null"]},
        "unit": {"type": ["string", "null"]},
        "source_label_raw": {"type": ["string", "null"]},
        "source_row_key": {"type": ["string", "null"]},
        "contract_reference_raw": {"type": ["string", "null"]},
        "semantic_summary_zh": {"type": ["string", "null"]},
        "model_derived_hints": {"type": "object"},
        "evidence": EXACT_EVIDENCE_SCHEMA,
        "semantic_synthesis": {"const": True},
        "review_status": {"const": "candidate"},
    },
    (
        "schema_version",
        "activity_id",
        "instrument_id",
        "subject_scope",
        "action",
        "object_raw",
        "object_id",
        "segment_id",
        "report_period",
        "value",
        "unit",
        "source_row_key",
        "contract_reference_raw",
        "evidence",
        "review_status",
    ),
)

SEMANTIC_VERIFICATION_SCHEMA = _closed_object(
    {
        "schema_version": {"const": "business_profile_semantic_verification.v1"},
        "verification_id": {"type": "string", "minLength": 1},
        "target_type": {
            "enum": ["activity", "relationship", "segment", "concentration"]
        },
        "target_id": {"type": "string", "minLength": 1},
        "decision": {"enum": ["confirmed", "conflict", "insufficient_evidence"]},
        "checks": _closed_object(
            {
                "subject": {"type": "boolean"},
                "action": {"type": "boolean"},
                "object": {"type": "boolean"},
                "scope": {"type": "boolean"},
                "period": {"type": "boolean"},
                "evidence": {"type": "boolean"},
            },
            ("subject", "action", "object", "scope", "period", "evidence"),
        ),
        "provider": {"type": "string", "minLength": 1},
        "actual_model": {"type": "string", "minLength": 1},
        "prompt_version": {"type": "string", "minLength": 1},
        "request_hash": _HASH,
        "response_hash": _HASH,
    },
    (
        "schema_version",
        "verification_id",
        "target_type",
        "target_id",
        "decision",
        "checks",
        "provider",
        "actual_model",
        "prompt_version",
        "request_hash",
        "response_hash",
    ),
)

PROMOTION_DECISION_SCHEMA = _closed_object(
    {
        "schema_version": {"const": "business_profile_promotion_decision.v1"},
        "target_type": {"type": "string", "minLength": 1},
        "target_id": {"type": "string", "minLength": 1},
        "classification": {
            "enum": ["auto_promoted", "machine_rework", "quick_review", "deep_review"]
        },
        "policy_version": {"type": "string", "minLength": 1},
        "gate_manifest_hash": _HASH,
        "reason_codes": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "uniqueItems": True,
        },
    },
    (
        "schema_version",
        "target_type",
        "target_id",
        "classification",
        "policy_version",
        "gate_manifest_hash",
        "reason_codes",
    ),
)

EXCEPTION_RECORD_SCHEMA = _closed_object(
    {
        "schema_version": {"const": "business_profile_exception.v1"},
        "exception_id": {"type": "string", "minLength": 1},
        "target_type": {"type": "string", "minLength": 1},
        "target_id": {"type": "string", "minLength": 1},
        "tier": {"enum": ["machine_rework", "quick_review", "deep_review"]},
        "reason_codes": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "minItems": 1,
            "uniqueItems": True,
        },
        "retry_count": {"type": "integer", "minimum": 0},
        "next_retry_at": {"type": ["string", "null"], "format": "date-time"},
        "gate_signature": _HASH,
        "resolved_at": {"type": ["string", "null"], "format": "date-time"},
    },
    (
        "schema_version",
        "exception_id",
        "target_type",
        "target_id",
        "tier",
        "reason_codes",
        "retry_count",
        "next_retry_at",
        "gate_signature",
        "resolved_at",
    ),
)

EXPOSURE_PUBLICATION_SCHEMA = _closed_object(
    {
        "schema_version": {"const": "business_profile_exposure_publication.v1"},
        "exposure_id": {"type": "string", "minLength": 1},
        "instrument_id": {"type": "string", "minLength": 1},
        "fact_ids": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "minItems": 1,
            "uniqueItems": True,
        },
        "mapping_ids": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "minItems": 1,
            "uniqueItems": True,
        },
        "assumption_ids": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "uniqueItems": True,
        },
        "direction_rule_id": {"type": "string", "minLength": 1},
        "build_policy_version": {"type": "string", "minLength": 1},
        "build_policy_hash": _HASH,
        "component_lineage_hash": _HASH,
        "effective_from": _OPTIONAL_DATE,
        "effective_to": _OPTIONAL_DATE,
        "knowledge_from": _DATE,
        "knowledge_to": _OPTIONAL_DATE,
    },
    (
        "schema_version",
        "exposure_id",
        "instrument_id",
        "fact_ids",
        "mapping_ids",
        "assumption_ids",
        "direction_rule_id",
        "build_policy_version",
        "build_policy_hash",
        "component_lineage_hash",
        "effective_from",
        "effective_to",
        "knowledge_from",
        "knowledge_to",
    ),
)

BUSINESS_PROFILE_SEMANTIC_SCHEMAS: dict[str, dict[str, Any]] = {
    "selected_section_bundle": SELECTED_SECTION_BUNDLE_SCHEMA,
    "atomic_activity": ATOMIC_ACTIVITY_SCHEMA,
    "semantic_verification": SEMANTIC_VERIFICATION_SCHEMA,
    "promotion_decision": PROMOTION_DECISION_SCHEMA,
    "exception_record": EXCEPTION_RECORD_SCHEMA,
    "exposure_publication": EXPOSURE_PUBLICATION_SCHEMA,
}


def get_business_profile_semantic_schema(artifact_type: str) -> dict[str, Any]:
    """Return a copy of one governed schema or fail on unknown artifact type."""

    key = str(artifact_type or "").strip()
    try:
        return deepcopy(BUSINESS_PROFILE_SEMANTIC_SCHEMAS[key])
    except KeyError as exc:
        raise ValueError(f"unsupported business-profile artifact type: {key}") from exc


def validate_business_profile_artifact(
    artifact_type: str,
    payload: Mapping[str, Any],
) -> None:
    """Validate one artifact against its closed versioned schema."""

    if not isinstance(payload, Mapping):
        raise ValueError("business-profile artifact payload must be an object")
    schema = get_business_profile_semantic_schema(artifact_type)
    errors = sorted(
        Draft202012Validator(schema).iter_errors(dict(payload)),
        key=lambda item: tuple(str(part) for part in item.absolute_path),
    )
    if errors:
        error = errors[0]
        location = ".".join(str(part) for part in error.absolute_path) or "$"
        raise ValueError(
            f"invalid business-profile {artifact_type} at {location}: {error.message}"
        )


def business_profile_semantic_schema_manifest() -> dict[str, Any]:
    """Return stable schema identities for checkpoints and promotion manifests."""

    return {
        "schema_set_version": BUSINESS_PROFILE_SEMANTIC_SCHEMA_SET_VERSION,
        "artifact_schema_versions": {
            key: schema["properties"]["schema_version"]["const"]
            for key, schema in BUSINESS_PROFILE_SEMANTIC_SCHEMAS.items()
        },
    }
