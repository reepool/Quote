"""Common-gateway SemanticProvider adapter for the isolated stage-five slice."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Callable, Mapping
from copy import deepcopy
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

from utils.llm import (
    LlmClientProtocol,
    LlmError,
    LlmMessage,
    LlmRequest,
    LlmResponse,
    LlmResponseParseError,
    LlmSchemaValidationError,
)

from .contracts import (
    ContractErrorCode,
    ExtractResponse,
    RepairRequest,
    RepairResponse,
    SemanticProviderError,
    SemanticTaskRequest,
    VerifyRequest,
    VerifyResponse,
)
from .models import LogicalSlot, MetricType
from .stage5 import PreparedRequestScope
from .stage5_bundle import Stage5ProviderCallTrace

_ResponseT = TypeVar("_ResponseT", bound=BaseModel)

_TASK_INSTRUCTIONS = {
    "extract_business_overview": (
        "Extract the report's business overview as source-native text. "
        "BusinessOverview.source_text must be a contiguous substring of one supplied "
        "text Evidence bounded_quote after whitespace normalization. Only emit Activity "
        "when the source sentence directly states an allowed business action and actor. "
        "For Activity, activity_actor and source_actor must be the same exact source-native "
        "actor wording; do not normalize 公司 into an issuer name or generic actor. "
        "Use actor_basis=direct_grammatical_actor when that actor directly governs the "
        "source verb, including coordinated verbs in the same sentence. Use "
        "explicit_economic_relationship only when the source explicitly states the actor "
        "through an economic relationship rather than as the grammatical verb subject; "
        "omit the Activity when neither basis is explicit."
    ),
    "extract_segment_financials": (
        "Keep one Segment/Measurement per physical table row and preserve source-native "
        "labels, units, and row_class. When the response schema omits dimension, return "
        "only the source row label; the evidence plan binds that label to its complete "
        "source-native dimension heading locally. Never add or rewrite a dimension field. "
        "Use one subject decision for every row in this table. When primary context "
        "contains the table revenue total and subject Evidence contains consolidated and "
        "parent-company income-statement revenue, compare the exact source values. If the "
        "table total equals consolidated revenue and differs from parent-company revenue, "
        "use subject_scope=consolidated_group, subject_basis="
        "numeric_reconciliation_to_consolidated_statement, and non-empty uncertainty "
        "listing all three source values. Do not calculate or infer missing values. Every "
        "candidate must cite the Evidence page whose bounded text contains that exact row; "
        "for a continued table, do not cite the opening page for a row printed on the "
        "continuation page. Emit only rows whose labels are literally printed in the "
        "supplied table; never invent residual, other, subtotal, or total rows to reconcile "
        "the numbers."
    ),
    "extract_operating_quantities": (
        "Distinguish production, sales, inventory, capacity, and processing_volume by "
        "the source label; processing_volume is only external service provided."
    ),
    "extract_material_inputs": (
        "Extract only explicitly named material inputs and their stated relationship; "
        "do not infer materials from industry knowledge."
    ),
    "extract_counterparties_and_concentration": (
        "Keep named, report-local anonymous, and report-local aggregate identities "
        "separate; concentration alone must not create a Relationship."
    ),
    "extract_business_regime": (
        "Record only explicitly disclosed business or control changes and effective dates; "
        "do not overwrite predecessor facts with restated comparisons."
    ),
}


_METRIC_LOGICAL_SLOTS = {
    MetricType.OPERATING_REVENUE.value: LogicalSlot.REVENUE.value,
    MetricType.OPERATING_COST.value: LogicalSlot.COST.value,
    MetricType.GROSS_MARGIN_REPORTED.value: LogicalSlot.GROSS_MARGIN.value,
    MetricType.PRODUCTION_CAPACITY.value: LogicalSlot.CAPACITY.value,
    MetricType.CAPACITY_UNDER_CONSTRUCTION.value: (
        LogicalSlot.CAPACITY_UNDER_CONSTRUCTION.value
    ),
    MetricType.CAPACITY_UTILIZATION.value: LogicalSlot.CAPACITY_UTILIZATION.value,
    MetricType.PRODUCTION_VOLUME.value: LogicalSlot.PRODUCTION_VOLUME.value,
    MetricType.SALES_VOLUME.value: LogicalSlot.SALES_VOLUME.value,
    MetricType.INVENTORY_VOLUME.value: LogicalSlot.INVENTORY_VOLUME.value,
    MetricType.PROCESSING_VOLUME.value: LogicalSlot.PROCESSING_VOLUME.value,
    MetricType.CUSTOMER_SALES_AMOUNT.value: LogicalSlot.CUSTOMER_SALES_AMOUNT.value,
    MetricType.SUPPLIER_PURCHASE_AMOUNT.value: (
        LogicalSlot.SUPPLIER_PURCHASE_AMOUNT.value
    ),
    MetricType.DISCLOSED_SHARE.value: LogicalSlot.DISCLOSED_SHARE.value,
}
_MECHANICAL_RECORD_FIELDS = {
    "schema_version",
    "record_id",
    "chapter_task",
    "report",
    "assertion_class",
    "evidence",
    "data_status",
}
_RECORD_IDENTITY_FIELDS = {
    "BusinessOverview": ("source_text",),
    "Segment": ("dimension", "label", "row_class"),
    "Activity": ("action", "activity_actor", "object_name", "source_verb"),
    "Measurement": (
        "metric_type",
        "measured_object",
        "segment_dimension",
        "segment_label",
        "row_class",
        "relationship_context",
    ),
    "Relationship": ("relation_type", "object_name", "identity_class"),
    "BusinessEvent": ("event_type", "event_date", "description"),
    "BusinessRegime": ("regime_label", "effective_from"),
    "IndustryPackageAssignment": ("package_name", "package_version", "effective_from"),
}


def _compact_extract_runtime_payload(request: SemanticTaskRequest) -> dict[str, Any]:
    """Send only unresolved semantics and one deduplicated Evidence catalog."""

    active = {
        item.field_id: item
        for item in request.package_manifest.active_items(request.chapter_task)
    }
    payload: dict[str, Any] = {
        "request_id": request.request_id,
        "unresolved_field_ids": list(request.unresolved_field_ids),
        "checklist": [
            {
                "field_id": field_id,
                "object_type": active[field_id].object_type.value,
                "requirement_level": active[field_id].requirement_level.value,
                "allowed_coverage_statuses": [
                    status.value
                    for status in active[field_id].allowed_coverage_statuses
                    if status.value != "observed"
                ],
                "allowed_metric_types": [
                    metric.value for metric in active[field_id].allowed_metric_types
                ],
                "allowed_actions": [
                    action.value for action in active[field_id].allowed_actions
                ],
            }
            for field_id in request.unresolved_field_ids
        ],
        "prohibited_inferences": list(request.prohibited_inferences),
        "evidence_catalog": _evidence_catalog(
            request.evidence_bundle,
            include_bounded_quotes=False,
        ),
    }
    if request.deterministic_candidates:
        payload["deterministic_candidates"] = [
            _candidate_to_draft(item, include_record_id=True)
            for item in request.deterministic_candidates
        ]
    if request.provided_coverage:
        payload["provided_coverage"] = [
            _coverage_to_draft(item) for item in request.provided_coverage
        ]
    return payload


def _compact_repair_runtime_payload(request: RepairRequest) -> dict[str, Any]:
    return {
        "request_id": request.request_id,
        "original_request_id": request.original_request_id,
        "error_code": request.error_code.value,
        "writable_fields": list(request.writable_fields),
        "original_candidate": _candidate_to_draft(
            request.original_candidate,
            include_record_id=True,
        ),
        "evidence_catalog": _evidence_catalog(
            request.evidence_bundle,
            include_bounded_quotes=True,
        ),
    }


def _compact_verify_runtime_payload(request: VerifyRequest) -> dict[str, Any]:
    return {
        "request_id": request.request_id,
        "original_request_id": request.original_request_id,
        "report_id": request.report.report_id,
        "evidence_catalog": _evidence_catalog(
            request.evidence_bundle,
            include_bounded_quotes=True,
        ),
        "candidates": [
            _candidate_to_draft(item, include_record_id=True)
            for item in request.candidates
        ],
        "coverage": [
            {
                "target_id": f"{item.chapter_task.value}:{item.field_id}",
                **_coverage_to_draft(item),
            }
            for item in request.coverage
        ],
    }


def _evidence_catalog(
    evidence_bundle: Any,
    *,
    include_bounded_quotes: bool,
) -> list[dict[str, Any]]:
    catalog: dict[str, dict[str, Any]] = {}
    for prepared in evidence_bundle:
        evidence = prepared.evidence
        item = catalog.setdefault(
            evidence.evidence_id,
            {
                "evidence_id": evidence.evidence_id,
                "page": evidence.page,
                "section_title": evidence.section_title,
                "anchor": _compact_anchor(
                    evidence.anchor.model_dump(mode="json"),
                    include_bounded_quotes=include_bounded_quotes,
                ),
                "field_ids": [],
            },
        )
        if prepared.field_id and prepared.field_id not in item["field_ids"]:
            item["field_ids"].append(prepared.field_id)
        if prepared.source_native is not None:
            bindings = item.setdefault("source_bindings", [])
            binding = {
                "field_id": prepared.field_id,
                "source_native": prepared.source_native.model_dump(mode="json"),
            }
            if binding not in bindings:
                bindings.append(binding)
    return list(catalog.values())


def _compact_anchor(
    anchor: dict[str, Any],
    *,
    include_bounded_quotes: bool,
) -> dict[str, Any]:
    if anchor.get("anchor_type") == "text" and not include_bounded_quotes:
        return {"anchor_type": "text"}
    return {
        key: value for key, value in anchor.items() if value not in (None, "", [], {})
    }


def _candidate_to_draft(candidate: Any, *, include_record_id: bool) -> dict[str, Any]:
    payload = candidate.model_dump(mode="json")
    evidence = payload.pop("evidence", [])
    for key in _MECHANICAL_RECORD_FIELDS - {"evidence", "record_id"}:
        payload.pop(key, None)
    if not include_record_id:
        payload.pop("record_id", None)
    payload["evidence_ids"] = [item["evidence_id"] for item in evidence]
    return payload


def _coverage_to_draft(coverage: Any) -> dict[str, Any]:
    payload = coverage.model_dump(mode="json")
    evidence = payload.pop("evidence", [])
    for key in ("schema_version", "chapter_task", "requirement_level"):
        payload.pop(key, None)
    payload["evidence_ids"] = [item["evidence_id"] for item in evidence]
    return payload


def _segment_dimension_options(
    prepared_scope: PreparedRequestScope | None,
) -> list[str]:
    if prepared_scope is None:
        return []
    options: list[str] = []
    saw_adjustment = False
    pattern = re.compile(
        r"(?:分(?:行业|业务|产品|地区|销售模式)|"
        r"按(?:行业|业务|产品|地区|销售模式)(?:分类分析|分类)?)"
    )
    for prepared in prepared_scope.evidence_bundle:
        evidence = prepared.evidence
        if evidence.section_title == "主体口径核对":
            continue
        source = str(getattr(evidence.anchor, "bounded_quote", ""))
        for match in pattern.finditer("".join(source.split())):
            option = match.group(0)
            if option not in options:
                options.append(option)
        if re.search(r"(?:合并.*抵[消销]|抵[消销]项)", source):
            saw_adjustment = True
    if saw_adjustment:
        options.append("adjustment")
    return options


def _segment_row_extract_schema(
    request: SemanticTaskRequest,
    *,
    prepared_scope: PreparedRequestScope | None = None,
) -> dict[str, Any]:
    field_ids = list(request.unresolved_field_ids)
    metric_cells = {
        "operating_revenue": "operating_revenue",
        "operating_cost": "operating_cost",
        "gross_margin_reported": "gross_margin_reported",
    }
    cell_properties = {
        cell_name: _segment_cell_schema()
        for field_id, cell_name in metric_cells.items()
        if field_id in field_ids
    }
    source_row_dimensions = (
        prepared_scope.source_row_dimensions if prepared_scope is not None else {}
    )
    dimension_options = (
        [] if source_row_dimensions else _segment_dimension_options(prepared_scope)
    )
    candidate_evidence_ids = (
        [
            item.evidence.evidence_id
            for item in prepared_scope.evidence_bundle
            if item.evidence.page in prepared_scope.candidate_pages
        ]
        if prepared_scope is not None and prepared_scope.candidate_pages
        else []
    )
    required = [
        "label",
        "subject_scope",
        "reported_period",
        "period_type",
        "evidence_ids",
        "cells",
    ]
    if not source_row_dimensions:
        required.insert(0, "dimension")
    row_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": required,
        "properties": {
            "label": (
                {"enum": list(source_row_dimensions)}
                if source_row_dimensions
                else {"type": "string"}
            ),
            "row_class": {"enum": ["consolidation_adjustment"]},
            "subject_scope": {
                "enum": [
                    "consolidated_group",
                    "issuer",
                    "named_subsidiary",
                    "business_segment",
                    "unclear",
                ]
            },
            "subject_name": {"type": "string"},
            "subject_basis": {
                "enum": [
                    "direct_source_wording",
                    "numeric_reconciliation_to_consolidated_statement",
                    "unclear",
                ]
            },
            "reported_period": {"type": "string"},
            "period_type": {"enum": ["duration", "instant", "event", "expected"]},
            "knowledge_time": {"type": "string"},
            "uncertainty": {"type": "array", "items": {"type": "string"}},
            "evidence_ids": {
                "type": "array",
                "minItems": 1,
                "items": (
                    {"enum": candidate_evidence_ids}
                    if candidate_evidence_ids
                    else {"type": "string"}
                ),
            },
            "cells": {
                "type": "object",
                "additionalProperties": False,
                "minProperties": 1,
                "properties": cell_properties,
            },
        },
    }
    if not source_row_dimensions:
        row_schema["properties"]["dimension"] = (
            {"enum": dimension_options} if dimension_options else {"type": "string"}
        )
    item_schemas: list[dict[str, Any]] = [
        {
            "type": "object",
            "additionalProperties": False,
            "required": ["item_type", "row"],
            "properties": {
                "item_type": {"const": "segment_row"},
                "row": row_schema,
            },
        }
    ]
    coverage_statuses = _allowed_non_observed_coverage_statuses(request)
    if coverage_statuses:
        item_schemas.append(
            {
                "type": "object",
                "additionalProperties": False,
                "required": ["item_type", "coverage"],
                "properties": {
                    "item_type": {"const": "coverage"},
                    "coverage": _coverage_draft_schema(
                        field_ids=field_ids,
                        statuses=coverage_statuses,
                    ),
                },
            }
        )
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "request_id", "items"],
        "properties": {
            "schema_version": {"const": "company_profile_extract_response.v1"},
            "request_id": {"type": "string"},
            "items": {"type": "array", "items": {"oneOf": item_schemas}},
        },
    }


def _segment_cell_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["value", "unit", "header"],
        "properties": {
            "value": {"type": "string"},
            "unit": {"type": "string"},
            "header": {"type": "string"},
            "qualifier": {"type": "string"},
            "footnote_refs": {"type": "array", "items": {"type": "string"}},
            "source_aliases": {"type": "array", "items": {"type": "string"}},
        },
    }


def _allowed_non_observed_coverage_statuses(
    request: SemanticTaskRequest,
) -> list[str]:
    return sorted(
        {
            status.value
            for item in request.package_manifest.active_items(request.chapter_task)
            if item.field_id in request.unresolved_field_ids
            for status in item.allowed_coverage_statuses
            if status.value != "observed"
        }
    )


def _minimal_extract_schema(
    request: SemanticTaskRequest,
    *,
    prepared_scope: PreparedRequestScope | None = None,
) -> dict[str, Any]:
    if request.chapter_task.value == "extract_segment_financials":
        return _segment_row_extract_schema(request, prepared_scope=prepared_scope)
    field_ids = list(request.unresolved_field_ids)
    candidate_types = [item.value for item in request.allowed_object_types]
    allowed_metric_types = [item.value for item in request.allowed_metric_types]
    allowed_actions = [item.value for item in request.allowed_actions]
    candidates: list[dict[str, Any]] = []
    for object_type in candidate_types:
        metric_groups = (
            ([metric_type] for metric_type in allowed_metric_types)
            if object_type == "Measurement"
            else (allowed_metric_types,)
        )
        candidates.extend(
            _candidate_draft_schema(
                object_type,
                field_ids=field_ids,
                allowed_metric_types=metric_group,
                allowed_actions=allowed_actions,
            )
            for metric_group in metric_groups
        )
    item_schemas: list[dict[str, Any]] = [
        {
            "type": "object",
            "additionalProperties": False,
            "required": ["item_type", "candidate"],
            "properties": {
                "item_type": {"const": "candidate"},
                "candidate": {"oneOf": candidates},
            },
        }
    ]
    coverage_statuses = _allowed_non_observed_coverage_statuses(request)
    if coverage_statuses:
        item_schemas.append(
            {
                "type": "object",
                "additionalProperties": False,
                "required": ["item_type", "coverage"],
                "properties": {
                    "item_type": {"const": "coverage"},
                    "coverage": _coverage_draft_schema(
                        field_ids=field_ids,
                        statuses=coverage_statuses,
                    ),
                },
            }
        )
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "request_id", "items"],
        "properties": {
            "schema_version": {"const": "company_profile_extract_response.v1"},
            "request_id": {"type": "string"},
            "items": {"type": "array", "items": {"oneOf": item_schemas}},
        },
    }


def _candidate_draft_schema(
    object_type: str,
    *,
    field_ids: list[str],
    allowed_metric_types: list[str],
    allowed_actions: list[str],
) -> dict[str, Any]:
    properties: dict[str, Any] = {
        "object_type": {"const": object_type},
        "field_id": {"enum": field_ids},
        "subject_scope": {
            "enum": [
                "consolidated_group",
                "issuer",
                "named_subsidiary",
                "business_segment",
                "unclear",
            ]
        },
        "subject_name": {"type": "string"},
        "subject_basis": {
            "enum": [
                "direct_source_wording",
                "numeric_reconciliation_to_consolidated_statement",
                "direct_grammatical_actor",
                "explicit_economic_relationship",
                "unclear",
            ]
        },
        "reported_period": {"type": "string"},
        "period_type": {"enum": ["duration", "instant", "event", "expected"]},
        "knowledge_time": {"type": "string"},
        "source_native": _source_native_schema(),
        "uncertainty": {"type": "array", "items": {"type": "string"}},
        "evidence_ids": {
            "type": "array",
            "minItems": 1,
            "items": {"type": "string"},
        },
    }
    required = [
        "object_type",
        "field_id",
        "subject_scope",
        "reported_period",
        "period_type",
        "source_native",
        "evidence_ids",
    ]
    extras: dict[str, dict[str, Any]] = {
        "BusinessOverview": {"source_text": {"type": "string"}},
        "Segment": {
            "dimension": {"type": "string"},
            "label": {"type": "string"},
            "row_class": {"enum": ["consolidation_adjustment"]},
        },
        "Activity": {
            "action": {"enum": allowed_actions},
            "activity_actor": {"type": "string"},
            "source_actor": {"type": "string"},
            "actor_basis": {
                "enum": ["direct_grammatical_actor", "explicit_economic_relationship"]
            },
            "object_name": {"type": "string"},
            "source_verb": {"type": "string"},
        },
        "Measurement": {
            "metric_type": {"enum": allowed_metric_types},
            "measured_object": {"type": "string"},
            "segment_dimension": {"type": "string"},
            "segment_label": {"type": "string"},
            "capacity_kind": {
                "enum": [
                    "report_period_capacity",
                    "effective_capacity",
                    "design_capacity",
                    "source_native_other",
                    "unclear",
                ]
            },
            "processing_direction": {"enum": ["external_service_provided"]},
            "row_class": {"enum": ["consolidation_adjustment"]},
            "is_restated_comparative": {"type": "boolean"},
            "comparison_basis": {
                "enum": [
                    "current_period_after_restructuring",
                    "same_control_restated",
                    "original_as_published",
                    "source_native_other",
                    "unclear",
                ]
            },
            "relationship_context": {"type": "string"},
        },
        "Relationship": {
            "relation_type": {
                "enum": [
                    "customer",
                    "supplier",
                    "material_input",
                    "related_party",
                    "contract_counterparty",
                ]
            },
            "object_name": {"type": "string"},
            "identity_class": {
                "enum": [
                    "named",
                    "report_local_anonymous",
                    "report_local_aggregate",
                ]
            },
            "external_entity_id": {"type": "string"},
        },
        "BusinessEvent": {
            "event_type": {"type": "string"},
            "description": {"type": "string"},
            "event_date": {"type": "string"},
            "regime_effective_at": {"type": "string"},
            "comparison_basis": {"type": "string"},
        },
        "BusinessRegime": {
            "regime_label": {"type": "string"},
            "effective_from": {"type": "string"},
            "effective_to": {"type": "string"},
        },
        "IndustryPackageAssignment": {
            "package_name": {"type": "string"},
            "package_version": {"type": "string"},
            "effective_from": {"type": "string"},
            "effective_to": {"type": "string"},
        },
    }[object_type]
    required_extras = {
        "BusinessOverview": ["source_text"],
        "Segment": ["dimension", "label"],
        "Activity": [
            "action",
            "activity_actor",
            "source_actor",
            "actor_basis",
            "object_name",
            "source_verb",
        ],
        "Measurement": ["metric_type", "measured_object"],
        "Relationship": ["relation_type", "object_name"],
        "BusinessEvent": ["event_type", "description"],
        "BusinessRegime": ["regime_label", "effective_from"],
        "IndustryPackageAssignment": [
            "package_name",
            "package_version",
            "effective_from",
        ],
    }[object_type]
    if object_type == "Measurement":
        metric_type = (
            allowed_metric_types[0] if len(allowed_metric_types) == 1 else None
        )
        if metric_type in field_ids:
            properties["field_id"] = {"const": metric_type}
        if metric_type == MetricType.PRODUCTION_CAPACITY.value:
            required_extras.append("capacity_kind")
        else:
            extras.pop("capacity_kind", None)
        if metric_type == MetricType.PROCESSING_VOLUME.value:
            required_extras.append("processing_direction")
        else:
            extras.pop("processing_direction", None)
    properties.update(extras)
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [*required, *required_extras],
        "properties": properties,
    }


def _source_native_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "name": {"type": "string"},
            "value": {"type": "string"},
            "unit": {"type": "string"},
            "header": {"type": "string"},
            "qualifier": {"type": "string"},
            "footnote_refs": {"type": "array", "items": {"type": "string"}},
            "source_aliases": {"type": "array", "items": {"type": "string"}},
        },
    }


def _coverage_draft_schema(
    *,
    field_ids: list[str],
    statuses: list[str],
) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["field_id", "status"],
        "properties": {
            "field_id": {"enum": field_ids},
            "status": {"enum": statuses},
            "reason_code": {
                "enum": [
                    "explicit_confidentiality",
                    "explicit_disclosure_exemption",
                    "source_reason_unspecified",
                    "table_context_incomplete",
                    "source_unreadable",
                    "coverage_budget_exhausted",
                    "unit_ambiguous",
                    "required_result_missing",
                    "candidate_unresolved",
                    "source_explicitly_not_applicable",
                ]
            },
            "reason": {"type": "string"},
            "evidence_ids": {"type": "array", "items": {"type": "string"}},
            "reason_evidence_text": {"type": "string"},
        },
    }


def _minimal_repair_schema(request: RepairRequest) -> dict[str, Any]:
    writable = [item.removeprefix("/") for item in request.writable_fields]
    properties: dict[str, Any] = {}
    for field in writable:
        if field == "capacity_kind":
            properties[field] = {
                "enum": [
                    "report_period_capacity",
                    "effective_capacity",
                    "design_capacity",
                    "source_native_other",
                    "unclear",
                ]
            }
        else:
            properties[field] = {"type": ["string", "number", "boolean", "null"]}
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema_version", "request_id", "updates", "changed_fields"],
        "properties": {
            "schema_version": {"const": "company_profile_repair_response.v1"},
            "request_id": {"type": "string"},
            "updates": {
                "type": "object",
                "additionalProperties": False,
                "minProperties": 1,
                "properties": properties,
            },
            "changed_fields": {
                "type": "array",
                "minItems": 1,
                "items": {"enum": list(request.writable_fields)},
            },
        },
    }


def _validate_segment_row_source_labels(
    row: Mapping[str, Any],
    *,
    prepared_scope: PreparedRequestScope,
) -> str:
    evidence_ids = row.get("evidence_ids")
    if not isinstance(evidence_ids, list) or not evidence_ids:
        raise ValueError("segment row requires cited Evidence")
    selected = {str(evidence_id) for evidence_id in evidence_ids}
    source_text = " ".join(
        str(getattr(item.evidence.anchor, "bounded_quote", ""))
        for item in prepared_scope.evidence_bundle
        if item.evidence.evidence_id in selected
    )
    normalized_source = "".join(source_text.split())
    label = str(row.get("label") or "")
    if not label or "".join(label.split()) not in normalized_source:
        raise ValueError(
            f"segment row label must occur in its cited Evidence: {label!r}"
        )
    planned_dimension = prepared_scope.source_row_dimensions.get(label)
    if prepared_scope.source_row_dimensions and planned_dimension is None:
        raise ValueError(
            f"segment row label has no approved physical dimension mapping: {label!r}"
        )
    if planned_dimension is not None and row.get("dimension") not in (None, ""):
        raise ValueError(
            "provider must not return dimension when the evidence plan binds it locally"
        )
    dimension = planned_dimension or str(row.get("dimension") or "")
    if row.get("row_class") == "consolidation_adjustment":
        return dimension
    normalized_dimension = "".join(dimension.split())
    if not dimension or normalized_dimension not in normalized_source:
        raise ValueError(
            f"segment dimension must occur in its cited Evidence: {dimension!r}"
        )
    normalized_lines = {
        "".join(line.split()) for line in source_text.splitlines() if line.strip()
    }
    if (
        normalized_dimension not in normalized_lines
        and not normalized_dimension.startswith(("分", "按"))
    ):
        raise ValueError(
            f"segment dimension must preserve the full source heading: {dimension!r}"
        )
    return dimension


def _require_numeric_reconciliation_uncertainty(
    candidate: Mapping[str, Any],
) -> None:
    if (
        candidate.get("subject_basis")
        != "numeric_reconciliation_to_consolidated_statement"
    ):
        return
    uncertainty = candidate.get("uncertainty")
    if not isinstance(uncertainty, (list, tuple)) or not any(
        str(item).strip() for item in uncertainty
    ):
        raise ValueError(
            "numeric reconciliation subject basis requires non-empty uncertainty"
        )


def _expand_segment_row_draft(
    row: dict[str, Any],
    *,
    request: SemanticTaskRequest,
    prepared_scope: PreparedRequestScope,
) -> list[dict[str, Any]]:
    _require_numeric_reconciliation_uncertainty(row)
    dimension = _validate_segment_row_source_labels(row, prepared_scope=prepared_scope)
    common_keys = (
        "subject_scope",
        "subject_name",
        "subject_basis",
        "reported_period",
        "period_type",
        "knowledge_time",
        "uncertainty",
        "evidence_ids",
    )
    common = {key: deepcopy(row[key]) for key in common_keys if key in row}
    label = row.get("label")
    row_class = row.get("row_class")
    segment_source = {"name": label, "header": dimension}
    segment = {
        **common,
        "object_type": "Segment",
        "field_id": "segment_dimension",
        "source_native": segment_source,
        "dimension": dimension,
        "label": label,
    }
    if row_class is not None:
        segment["row_class"] = row_class
    candidates = [segment]
    metric_contract = {
        "operating_revenue": ("operating_revenue", "operating_revenue"),
        "operating_cost": ("operating_cost", "operating_cost"),
        "gross_margin_reported": (
            "gross_margin_reported",
            "gross_margin_reported",
        ),
    }
    cells = row.get("cells")
    if isinstance(cells, Mapping):
        for cell_name, (field_id, metric_type) in metric_contract.items():
            cell = cells.get(cell_name)
            if not isinstance(cell, Mapping):
                continue
            source_native = {"name": label, **deepcopy(dict(cell))}
            measurement = {
                **common,
                "object_type": "Measurement",
                "field_id": field_id,
                "source_native": source_native,
                "metric_type": metric_type,
                "measured_object": label,
                "segment_dimension": dimension,
                "segment_label": label,
            }
            if row_class is not None:
                measurement["row_class"] = row_class
            candidates.append(measurement)
    return [
        {
            "item_type": "candidate",
            "candidate": _expand_candidate_draft(
                candidate,
                request=request,
                prepared_scope=prepared_scope,
            ),
        }
        for candidate in candidates
    ]


def _expand_extract_response(
    data: Any,
    *,
    request: SemanticTaskRequest,
    prepared_scope: PreparedRequestScope,
) -> Any:
    if not isinstance(data, Mapping):
        return data
    result = deepcopy(dict(data))
    expanded_items: list[Any] = []
    for item in result.get("items", []):
        if not isinstance(item, dict):
            expanded_items.append(item)
            continue
        if item.get("item_type") == "segment_row" and isinstance(item.get("row"), dict):
            expanded_items.extend(
                _expand_segment_row_draft(
                    item["row"],
                    request=request,
                    prepared_scope=prepared_scope,
                )
            )
            continue
        if isinstance(item.get("candidate"), dict):
            item["candidate"] = _expand_candidate_draft(
                item["candidate"],
                request=request,
                prepared_scope=prepared_scope,
            )
        if isinstance(item.get("coverage"), dict):
            item["coverage"] = _expand_coverage_draft(
                item["coverage"],
                request=request,
                prepared_scope=prepared_scope,
            )
        expanded_items.append(item)
    result["items"] = expanded_items
    return result


def _expand_repair_response(
    data: Any,
    *,
    request: RepairRequest,
    prepared_scope: PreparedRequestScope,
) -> Any:
    if not isinstance(data, Mapping):
        return data
    result = deepcopy(dict(data))
    if isinstance(result.get("candidate"), dict):
        result["candidate"] = _expand_existing_fact_refs(
            result["candidate"], prepared_scope=prepared_scope
        )
        return result
    updates = result.pop("updates", None)
    changed_fields = result.get("changed_fields")
    if not isinstance(updates, Mapping) or not isinstance(changed_fields, list):
        return result
    expected_fields = {item.removeprefix("/") for item in request.writable_fields}
    if set(updates) - expected_fields:
        raise ValueError("repair response updated a field outside writable_fields")
    if {f"/{item}" for item in updates} != set(changed_fields):
        raise ValueError("repair changed_fields must exactly match returned updates")
    candidate = request.original_candidate.model_dump(mode="json")
    candidate.update(updates)
    _require_numeric_reconciliation_uncertainty(candidate)
    result["candidate"] = candidate
    return result


def _expand_candidate_draft(
    draft: dict[str, Any],
    *,
    request: SemanticTaskRequest,
    prepared_scope: PreparedRequestScope,
) -> dict[str, Any]:
    if {"schema_version", "record_id", "report", "evidence"}.issubset(draft):
        return _expand_existing_fact_refs(draft, prepared_scope=prepared_scope)
    candidate = deepcopy(draft)
    _require_numeric_reconciliation_uncertainty(candidate)
    evidence_ids = candidate.pop("evidence_ids", None)
    candidate["schema_version"] = "company_profile_semantic_object.v1"
    candidate["record_id"] = _stable_record_id(
        candidate,
        evidence_ids=evidence_ids,
        prepared_scope=prepared_scope,
    )
    candidate["chapter_task"] = request.chapter_task.value
    candidate["report"] = prepared_scope.report.model_dump(mode="json")
    candidate["assertion_class"] = "reported_fact"
    candidate["evidence"] = _canonical_evidence(
        evidence_ids,
        prepared_scope=prepared_scope,
    )
    candidate["data_status"] = "research_fixture"
    if candidate.get("object_type") == "Measurement":
        logical_slot = _METRIC_LOGICAL_SLOTS.get(str(candidate.get("metric_type")))
        if logical_slot is not None:
            candidate["logical_slot"] = logical_slot
    return candidate


def _expand_coverage_draft(
    draft: dict[str, Any],
    *,
    request: SemanticTaskRequest,
    prepared_scope: PreparedRequestScope,
) -> dict[str, Any]:
    if {"schema_version", "chapter_task", "requirement_level"}.issubset(draft):
        return _expand_existing_fact_refs(draft, prepared_scope=prepared_scope)
    coverage = deepcopy(draft)
    if coverage.get("status") == "observed":
        raise ValueError(
            "provider must not emit observed coverage; accepted candidates derive it"
        )
    field_id = str(coverage.get("field_id") or "")
    active = {
        item.field_id: item
        for item in request.package_manifest.active_items(request.chapter_task)
    }
    checklist = active.get(field_id)
    if checklist is None:
        return coverage
    evidence_ids = coverage.pop("evidence_ids", [])
    coverage["schema_version"] = "company_profile_coverage_result.v1"
    coverage["chapter_task"] = request.chapter_task.value
    coverage["requirement_level"] = checklist.requirement_level.value
    coverage["evidence"] = _canonical_evidence(
        evidence_ids,
        prepared_scope=prepared_scope,
    )
    return coverage


def _expand_existing_fact_refs(
    fact: dict[str, Any],
    *,
    prepared_scope: PreparedRequestScope,
) -> dict[str, Any]:
    result = deepcopy(fact)
    _require_numeric_reconciliation_uncertainty(result)
    report = result.get("report")
    if (
        isinstance(report, Mapping)
        and set(report) <= {"report_id"}
        and report.get("report_id") == prepared_scope.report.report_id
    ):
        result["report"] = prepared_scope.report.model_dump(mode="json")
    evidence = result.get("evidence")
    if isinstance(evidence, list):
        result["evidence"] = _canonical_evidence(
            [
                item.get("evidence_id") if isinstance(item, Mapping) else item
                for item in evidence
            ],
            prepared_scope=prepared_scope,
            preserve_full=evidence,
        )
    return result


def _canonical_evidence(
    evidence_ids: Any,
    *,
    prepared_scope: PreparedRequestScope,
    preserve_full: list[Any] | None = None,
) -> list[Any]:
    if not isinstance(evidence_ids, list):
        return evidence_ids
    expected = {
        item.evidence.evidence_id: item.evidence.model_dump(mode="json")
        for item in prepared_scope.evidence_bundle
    }
    expanded: list[Any] = []
    for index, evidence_id in enumerate(evidence_ids):
        canonical = expected.get(str(evidence_id))
        if canonical is not None:
            expanded.append(canonical)
        elif preserve_full is not None:
            expanded.append(preserve_full[index])
        else:
            expanded.append({"evidence_id": evidence_id})
    return expanded


def _stable_record_id(
    candidate: dict[str, Any],
    *,
    evidence_ids: Any,
    prepared_scope: PreparedRequestScope,
) -> str:
    object_type = str(candidate.get("object_type") or "unknown")
    material = {
        "instrument_id": prepared_scope.report.instrument_id,
        "document_version": prepared_scope.report.document_version,
        "chapter_task": prepared_scope.chapter_task.value,
        "field_id": candidate.get("field_id"),
        "object_type": object_type,
        "evidence_ids": sorted(evidence_ids or []),
        "identity": {
            key: candidate.get(key)
            for key in _RECORD_IDENTITY_FIELDS.get(object_type, ())
        },
        "source_native": candidate.get("source_native"),
    }
    encoded = json.dumps(
        material,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return f"stage5-{hashlib.sha256(encoded).hexdigest()[:24]}"


class CommonGatewaySemanticProvider:
    """Adapt one prepared request scope to the existing common LLM gateway.

    The adapter has no storage, package selection, approval, repair loop, or
    publication responsibility.  The stage-four semantic service remains the
    only owner of extract/repair/verify sequencing.
    """

    def __init__(
        self,
        *,
        client: LlmClientProtocol,
        profile: str,
        prepared_scope: PreparedRequestScope,
        max_output_tokens: int,
        timeout_seconds: float,
        runner: asyncio.Runner | None = None,
    ) -> None:
        if not profile.strip():
            raise ValueError("LLM profile is required")
        if max_output_tokens < 1:
            raise ValueError("max_output_tokens must be positive")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._client = client
        self._profile = profile
        self._prepared_scope = prepared_scope
        self._max_output_tokens = max_output_tokens
        self._timeout_seconds = timeout_seconds
        self._runner = runner
        self._traces: list[Stage5ProviderCallTrace] = []

    @property
    def traces(self) -> tuple[Stage5ProviderCallTrace, ...]:
        return tuple(self._traces)

    def extract(self, request: SemanticTaskRequest) -> Mapping[str, Any]:
        self._validate_extract_scope(request)
        return self._execute(
            call_type="extract",
            semantic_request_id=request.request_id,
            runtime_payload=_compact_extract_runtime_payload(request),
            response_model=ExtractResponse,
            schema_name="company_profile_extract_response",
            schema_version="company_profile_extract_response.v1",
            model_schema=_minimal_extract_schema(
                request, prepared_scope=self._prepared_scope
            ),
            normalize_response=lambda data: _expand_extract_response(
                data,
                request=request,
                prepared_scope=self._prepared_scope,
            ),
            include_page_contexts=True,
        )

    def repair(self, request: RepairRequest) -> Mapping[str, Any]:
        self._validate_evidence_identity(request.evidence_bundle)
        return self._execute(
            call_type="repair",
            semantic_request_id=request.request_id,
            runtime_payload=_compact_repair_runtime_payload(request),
            response_model=RepairResponse,
            schema_name="company_profile_repair_response",
            schema_version="company_profile_repair_response.v1",
            model_schema=_minimal_repair_schema(request),
            normalize_response=lambda data: _expand_repair_response(
                data,
                request=request,
                prepared_scope=self._prepared_scope,
            ),
            include_page_contexts=False,
        )

    def verify(self, request: VerifyRequest) -> Mapping[str, Any]:
        if request.report != self._prepared_scope.report:
            raise SemanticProviderError(
                ContractErrorCode.REQUEST_IDENTITY_MISMATCH,
                "verify report does not match the prepared request scope",
            )
        self._validate_evidence_identity(request.evidence_bundle)
        return self._execute(
            call_type="verify",
            semantic_request_id=request.request_id,
            runtime_payload=_compact_verify_runtime_payload(request),
            response_model=VerifyResponse,
            schema_name="company_profile_verify_response",
            schema_version="company_profile_verify_response.v1",
            model_schema=VerifyResponse,
            normalize_response=lambda data: data,
            include_page_contexts=False,
        )

    def _execute(
        self,
        *,
        call_type: str,
        semantic_request_id: str,
        runtime_payload: dict[str, Any],
        response_model: type[_ResponseT],
        schema_name: str,
        schema_version: str,
        model_schema: Any,
        normalize_response: Callable[[Any], Any],
        include_page_contexts: bool,
    ) -> dict[str, Any]:
        envelope = {
            "contract_version": "company_profile_manufacturing_materials_llm_contract.v1",
            "request_kind": call_type,
            "request_id": semantic_request_id,
            "request_scope": {
                "sample_id": self._prepared_scope.sample_id,
                "scope_id": self._prepared_scope.scope_id,
                "chapter_task": self._prepared_scope.chapter_task.value,
                "field_ids": list(self._prepared_scope.field_ids),
                "report": self._prepared_scope.report.model_dump(mode="json"),
                "task_instructions": _TASK_INSTRUCTIONS.get(
                    self._prepared_scope.chapter_task.value, ""
                ),
            },
            "runtime_request": runtime_payload,
            "boundaries": {
                "source_native_only": True,
                "production_authorization": "not_authorized",
                "may_choose_package": False,
                "may_publish": False,
                "may_approve": False,
                "json_only": True,
            },
        }
        if include_page_contexts:
            envelope["request_scope"]["page_contexts"] = [
                item.model_dump(mode="json")
                for item in self._prepared_scope.page_contexts
            ]
        llm_request = LlmRequest(
            profile=self._profile,
            messages=(
                LlmMessage(
                    role="system",
                    is_safety_instruction=True,
                    content=(
                        "You are a bounded company-profile semantic worker. "
                        "Use only the supplied PDF page context and runtime schema. "
                        "Return JSON only; never infer production approval, package "
                        "assignment, commodity exposure, value-chain position, or DCF input. "
                        "Return only semantic draft fields requested by the schema. Do not "
                        "repeat report, chapter_task, record_id, schema_version, assertion_class, "
                        "data_status, or full Evidence inside a candidate. Return evidence_ids "
                        "only; the adapter binds canonical source evidence locally. When a "
                        "candidate is emitted, do not also emit observed coverage for that field; "
                        "the workflow derives observed coverage after acceptance. The wording 公司 "
                        "alone does not prove consolidated_group: use subject_scope=unclear unless "
                        "the source explicitly says 合并/本集团 or the supplied evidence documents "
                        "numeric reconciliation to the consolidated statement. Every "
                        "consolidated_group candidate must include the matching subject_basis. "
                        "When subject_basis is numeric reconciliation, uncertainty must be "
                        "non-empty and state the source-table total plus its comparison with "
                        "the consolidated and parent-company statement values from Evidence."
                    ),
                ),
                LlmMessage(
                    role="user",
                    content=json.dumps(
                        envelope,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            ),
            # Keep the complete Pydantic model for local validation, but send
            # only the task/object-reachable schema to the provider.  The
            # configured route currently uses json_object, so a full model
            # schema would otherwise be embedded in the prompt.
            response_schema=model_schema,
            schema_name=schema_name,
            schema_version=schema_version,
            temperature=0,
            max_output_tokens=self._max_output_tokens,
            timeout_seconds=self._timeout_seconds,
            idempotency_key=f"{semantic_request_id}:{call_type}",
            metadata={
                "workload": "company_profile_stage5",
                "run_id": semantic_request_id.split(":", 1)[0],
                "stage": call_type,
                "business_item_key": (
                    f"{self._prepared_scope.sample_id}:{self._prepared_scope.scope_id}"
                ),
            },
            content_is_untrusted=True,
        )
        response: LlmResponse | None = None
        try:
            response = _run_complete(self._client, llm_request, self._runner)
            normalized_response = normalize_response(response.data)
            parsed = response_model.model_validate_json(
                json.dumps(normalized_response, ensure_ascii=False, allow_nan=False)
            )
            if parsed.request_id != semantic_request_id:
                raise SemanticProviderError(
                    ContractErrorCode.REQUEST_IDENTITY_MISMATCH,
                    f"{call_type} response request_id does not match the request",
                )
        except SemanticProviderError as exc:
            self._append_failure_trace(
                call_type, semantic_request_id, exc.code.value, str(exc)
            )
            raise
        except LlmError as exc:
            code = _contract_error_for_llm(exc)
            self._append_failure_trace(
                call_type,
                semantic_request_id,
                code.value,
                exc.message,
                gateway_request_id=exc.request_id,
            )
            raise SemanticProviderError(code, exc.message) from exc
        except (ValidationError, TypeError, ValueError) as exc:
            code = ContractErrorCode.CANDIDATE_SCHEMA_INVALID
            self._append_failure_trace(
                call_type,
                semantic_request_id,
                code.value,
                str(exc)[:2000],
                gateway_request_id=(
                    response.request_id if response is not None else None
                ),
            )
            raise SemanticProviderError(
                code, "gateway response violates the schema"
            ) from exc
        self._traces.append(
            Stage5ProviderCallTrace(
                call_type=call_type,
                semantic_request_id=semantic_request_id,
                gateway_request_id=response.request_id,
                status="success",
                profile=self._profile,
                provider=response.provider,
                model=response.model,
                response_hash=response.response_hash,
            )
        )
        return parsed.model_dump(mode="json")

    def _validate_extract_scope(self, request: SemanticTaskRequest) -> None:
        if (
            request.report != self._prepared_scope.report
            or request.chapter_task != self._prepared_scope.chapter_task
            or set(request.unresolved_field_ids) - set(self._prepared_scope.field_ids)
        ):
            raise SemanticProviderError(
                ContractErrorCode.REQUEST_IDENTITY_MISMATCH,
                "extract request is outside the bound prepared request scope",
            )
        self._validate_evidence_identity(request.evidence_bundle)

    def _validate_evidence_identity(self, evidence_bundle: Any) -> None:
        expected = {
            item.evidence.evidence_id for item in self._prepared_scope.evidence_bundle
        }
        actual = {item.evidence.evidence_id for item in evidence_bundle}
        if actual != expected:
            raise SemanticProviderError(
                ContractErrorCode.REQUEST_IDENTITY_MISMATCH,
                "semantic request Evidence differs from the prepared request scope",
            )

    def _append_failure_trace(
        self,
        call_type: str,
        semantic_request_id: str,
        error_code: str,
        error_detail: str,
        gateway_request_id: str | None = None,
    ) -> None:
        self._traces.append(
            Stage5ProviderCallTrace(
                call_type=call_type,
                semantic_request_id=semantic_request_id,
                gateway_request_id=gateway_request_id,
                status="failed",
                profile=self._profile,
                error_code=error_code,
                error_detail=error_detail[:2000],
            )
        )


def _run_complete(
    client: LlmClientProtocol,
    request: LlmRequest,
    runner: asyncio.Runner | None,
) -> LlmResponse:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        if runner is not None:
            return runner.run(client.complete(request))
        return asyncio.run(client.complete(request))
    raise SemanticProviderError(
        ContractErrorCode.PROVIDER_UNAVAILABLE,
        "synchronous stage-five provider cannot run inside an active event loop",
    )


def _contract_error_for_llm(error: LlmError) -> ContractErrorCode:
    if isinstance(error, (LlmResponseParseError, LlmSchemaValidationError)):
        return ContractErrorCode.CANDIDATE_SCHEMA_INVALID
    if error.code == "deadline_exceeded":
        return ContractErrorCode.DEADLINE_EXCEEDED
    return ContractErrorCode.PROVIDER_UNAVAILABLE
