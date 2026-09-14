"""Small, program-owned acceptance rules for research-only company profiles.

The policy deliberately reuses the semantic model's existing subject and evidence
fields.  It is not a confidence service or a persisted authorization layer.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from enum import Enum
from typing import Any

from .contracts import DispositionStatus
from .models import (
    Activity,
    BusinessEvent,
    BusinessOverview,
    BusinessRegime,
    IndustryPackageAssignment,
    Measurement,
    Relationship,
    Segment,
    SemanticRecord,
    SubjectBasis,
    SubjectScope,
)

_SELF_ACTORS = frozenset({"公司", "本公司", "本集团", "集团", "上市公司"})
_NARROWER_SCOPES = frozenset(
    {"issuer", "named_subsidiary", "business_segment"}
)
_DEFAULT_GROUP_BASES = frozenset({None, "", "unclear", "report_default_group_scope"})
_SUBJECT_ASSIGNMENT_FAILURE = re.compile(r"无法归属|不可区分")
_SUBJECT_SCOPE_CONFLICT = re.compile(
    r"(?:主体|口径|归属).{0,12}(?:冲突|conflict)"
    r"|(?:冲突|conflict).{0,12}(?:主体|口径|归属)",
    re.IGNORECASE,
)
_PARENT_OR_SEGMENT_SCOPE = re.compile(r"母公司|本公司单体|业务分部")
_GROUP_INCLUSIVE_SUBSIDIARY = re.compile(
    r"(?:本公司|本集团|公司)及(?:其)?(?:所属|全资|控股)?子公司"
)
_NAMED_SUBSIDIARY_PREFIX = re.compile(r"^(?:全资|控股|所属)?子公司[\u4e00-\u9fffA-Za-z0-9（）()]{1,20}公司")
_NAMED_SUBSIDIARY_FACT = re.compile(
    r"(?<![及和与])(?:全资|控股|所属)?子公司"
    r"[\u4e00-\u9fffA-Za-z0-9（）()]{1,20}公司"
    r"(?:的)?"
    r"(?:营业收入|营业成本|营业利润|营收|收入|成本|毛利|净利润|利润总额|产能|产量|销量)"
)

CORE_CHAPTERS = (
    "extract_business_overview",
    "extract_segment_financials",
    "extract_operating_quantities",
    "extract_material_inputs",
    "extract_counterparties_and_concentration",
    "extract_business_regime",
)

_GROUP_BASES = {
    SubjectBasis.DIRECT_SOURCE_WORDING,
    SubjectBasis.REPORT_DEFAULT_GROUP_SCOPE,
    SubjectBasis.NUMERIC_RECONCILIATION_TO_CONSOLIDATED_STATEMENT,
}

# The values are intentionally closed.  ``display`` is source-native research
# display; the other flags unlock only genuinely group-comparable uses.
_SENSITIVE_TYPES = (Measurement, Relationship)


def usage_policy(record: SemanticRecord) -> dict[str, bool | str]:
    """Return the closed downstream-use decision for one accepted record."""

    subject = record.subject_scope
    basis = record.subject_basis
    is_group = subject == SubjectScope.CONSOLIDATED_GROUP and basis in _GROUP_BASES
    if isinstance(
        record,
        (
            Activity,
            BusinessOverview,
            BusinessEvent,
            BusinessRegime,
            IndustryPackageAssignment,
        ),
    ):
        return {
            "display": True,
            "consolidated_aggregation": False,
            "ranking": False,
            "cross_company_comparison": False,
            "restriction_reason": "non_numeric_research_fact",
        }
    if isinstance(record, Segment):
        return {
            "display": True,
            "consolidated_aggregation": False,
            "ranking": False,
            "cross_company_comparison": False,
            "restriction_reason": "segment_identity_is_not_group_total",
        }
    if isinstance(record, _SENSITIVE_TYPES) and is_group:
        return {
            "display": True,
            "consolidated_aggregation": True,
            "ranking": True,
            "cross_company_comparison": True,
            "restriction_reason": "",
        }
    if isinstance(record, _SENSITIVE_TYPES):
        return {
            "display": True,
            "consolidated_aggregation": False,
            "ranking": False,
            "cross_company_comparison": False,
            "restriction_reason": (
                "unclear_subject_scope_restricted_from_consolidated_use"
                if subject == SubjectScope.UNCLEAR
                else "subject_scope_not_group_comparable"
            ),
        }
    return {
        "display": True,
        "consolidated_aggregation": False,
        "ranking": False,
        "cross_company_comparison": False,
        "restriction_reason": "subject_scope_not_group_comparable",
    }


def derive_confidence(
    record: SemanticRecord,
    *,
    disposition: DispositionStatus | str = DispositionStatus.ACCEPTED_FOR_REVIEW,
    contradiction: bool = False,
) -> str:
    """Derive a discrete confidence without weights or model-supplied scores."""

    status = disposition.value if isinstance(disposition, Enum) else str(disposition)
    if status != DispositionStatus.ACCEPTED_FOR_REVIEW.value:
        return "rejected"
    if contradiction:
        return "low"
    if not record.evidence:
        return "low"
    if record.subject_scope == SubjectScope.UNCLEAR:
        return "medium"
    if record.subject_basis in _GROUP_BASES or record.subject_basis in {
        SubjectBasis.DIRECT_GRAMMATICAL_ACTOR,
        SubjectBasis.EXPLICIT_ECONOMIC_RELATIONSHIP,
    }:
        return "high"
    if record.uncertainty:
        return "medium"
    return "medium"


def accepted_has_illegal_group_promotion(task_results: list[Any]) -> bool:
    """Detect a runtime record that claims group scope without affirmative basis."""

    for result in task_results:
        accepted_ids = {
            item.target_id
            for item in result.dispositions
            if item.status == DispositionStatus.ACCEPTED_FOR_REVIEW
        }
        for record in result.records:
            if (
                record.record_id in accepted_ids
                and record.subject_scope == SubjectScope.CONSOLIDATED_GROUP
                and not _has_affirmative_group_evidence(record)
            ):
                return True
    return False


def apply_report_default_group_scope(candidate: dict[str, Any]) -> dict[str, Any]:
    """Apply the report-level group convention to an otherwise unqualified draft."""

    scope = _scope_value(candidate.get("subject_scope"))
    basis = _scope_value(candidate.get("subject_basis"))
    if scope in _NARROWER_SCOPES:
        return candidate
    if (
        _object_type_value(candidate.get("object_type")) == "Segment"
        or candidate.get("row_class") == "consolidation_adjustment"
    ):
        return candidate
    if scope == "consolidated_group" and basis not in _DEFAULT_GROUP_BASES:
        return candidate
    if not report_default_group_is_legal(candidate):
        return _clear_default_group_subject(candidate)
    if scope in (None, "", "unclear"):
        candidate["subject_scope"] = "consolidated_group"
        candidate["subject_basis"] = "report_default_group_scope"
    return candidate


def report_default_group_is_legal(
    payload: SemanticRecord | Mapping[str, Any],
) -> bool:
    """Return whether default-group tags are allowed for this candidate's local evidence."""

    data = _as_subject_payload(payload)
    if _draft_is_third_party_activity(data):
        return False
    text = _local_subject_evidence_text(payload)
    if _has_subject_scope_conflict(text):
        return False
    return not _has_explicit_narrower_local_scope(payload, text)


def default_group_subject_is_unsupported(record: SemanticRecord) -> bool:
    """Return True when a default-group tag is not supported by local evidence."""

    if record.subject_scope != SubjectScope.CONSOLIDATED_GROUP:
        return False
    if record.subject_basis != SubjectBasis.REPORT_DEFAULT_GROUP_SCOPE:
        return False
    return not report_default_group_is_legal(record)


def activity_promotes_third_party_to_group(record: SemanticRecord) -> bool:
    """Return True when a non-issuer actor is treated as the listed-company group."""

    if not isinstance(record, Activity):
        return False
    if record.subject_scope != SubjectScope.CONSOLIDATED_GROUP:
        return False
    if record.subject_basis != SubjectBasis.REPORT_DEFAULT_GROUP_SCOPE:
        return False
    return any(
        actor not in _SELF_ACTORS
        for actor in (record.activity_actor.strip(), record.source_actor.strip())
        if actor
    )


def _scope_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, Enum):
        return value.value
    text = str(value).strip()
    return text or None


def _object_type_value(value: Any) -> str | None:
    return _scope_value(value)


def _as_subject_payload(
    payload: SemanticRecord | Mapping[str, Any],
) -> Mapping[str, Any]:
    if isinstance(payload, Mapping):
        return payload
    return {
        "object_type": payload.object_type,
        "activity_actor": getattr(payload, "activity_actor", None),
        "source_actor": getattr(payload, "source_actor", None),
    }


def _local_subject_evidence_text(
    payload: SemanticRecord | Mapping[str, Any],
) -> str:
    parts: list[str] = []
    if not isinstance(payload, Mapping):
        parts.extend(payload.uncertainty)
        parts.append(str(payload.source_native.name or ""))
        parts.append(str(payload.source_native.header or ""))
        parts.append(str(payload.subject_name or ""))
        if isinstance(payload, BusinessOverview):
            parts.append(payload.source_text)
        for evidence in payload.evidence:
            parts.append(evidence.section_title)
            anchor = evidence.anchor
            for key in (
                "bounded_quote",
                "row_label",
                "table_label",
                "column_header",
                "cell_locator",
            ):
                parts.append(str(getattr(anchor, key, "") or ""))
        return " ".join(part for part in parts if part)

    parts.extend(str(item) for item in payload.get("uncertainty") or () if item)
    source = payload.get("source_native") or {}
    if isinstance(source, Mapping):
        parts.append(str(source.get("name") or ""))
        parts.append(str(source.get("header") or ""))
    parts.append(str(payload.get("source_text") or ""))
    parts.append(str(payload.get("subject_name") or ""))
    for evidence in payload.get("evidence") or ():
        if not isinstance(evidence, Mapping):
            continue
        parts.append(str(evidence.get("section_title") or ""))
        anchor = evidence.get("anchor") or {}
        if isinstance(anchor, Mapping):
            for key in (
                "bounded_quote",
                "row_label",
                "table_label",
                "column_header",
                "cell_locator",
            ):
                parts.append(str(anchor.get(key) or ""))
    return " ".join(part for part in parts if part)


def _has_subject_scope_conflict(text: str) -> bool:
    return bool(
        _SUBJECT_ASSIGNMENT_FAILURE.search(text) or _SUBJECT_SCOPE_CONFLICT.search(text)
    )


def _has_explicit_narrower_local_scope(
    payload: SemanticRecord | Mapping[str, Any],
    text: str,
) -> bool:
    if _PARENT_OR_SEGMENT_SCOPE.search(text):
        return True
    remainder = _GROUP_INCLUSIVE_SUBSIDIARY.sub("", text)
    if _NAMED_SUBSIDIARY_FACT.search(remainder):
        return True
    return any(
        _NAMED_SUBSIDIARY_PREFIX.match(field.strip())
        for field in _local_subject_identity_fields(payload)
        if field and field.strip()
    )


def _local_subject_identity_fields(
    payload: SemanticRecord | Mapping[str, Any],
) -> list[str]:
    if not isinstance(payload, Mapping):
        fields = [
            str(payload.source_native.name or ""),
            str(payload.source_native.header or ""),
            str(payload.subject_name or ""),
        ]
        if isinstance(payload, BusinessOverview):
            fields.append(payload.source_text)
        for evidence in payload.evidence:
            fields.append(evidence.section_title)
            anchor = evidence.anchor
            for key in ("bounded_quote", "row_label", "table_label", "column_header"):
                fields.append(str(getattr(anchor, key, "") or ""))
        return fields

    source = payload.get("source_native") or {}
    fields = [
        str(source.get("name") or "") if isinstance(source, Mapping) else "",
        str(source.get("header") or "") if isinstance(source, Mapping) else "",
        str(payload.get("source_text") or ""),
        str(payload.get("subject_name") or ""),
    ]
    for evidence in payload.get("evidence") or ():
        if not isinstance(evidence, Mapping):
            continue
        fields.append(str(evidence.get("section_title") or ""))
        anchor = evidence.get("anchor") or {}
        if not isinstance(anchor, Mapping):
            continue
        for key in ("bounded_quote", "row_label", "table_label", "column_header"):
            fields.append(str(anchor.get(key) or ""))
    return fields


def _clear_default_group_subject(candidate: dict[str, Any]) -> dict[str, Any]:
    candidate["subject_scope"] = "unclear"
    if _scope_value(candidate.get("subject_basis")) == "report_default_group_scope":
        candidate.pop("subject_basis", None)
    return candidate


def _draft_is_third_party_activity(candidate: Mapping[str, Any]) -> bool:
    if _object_type_value(candidate.get("object_type")) != "Activity":
        return False
    actors = [
        str(candidate.get(key) or "").strip()
        for key in ("activity_actor", "source_actor")
    ]
    return any(actor and actor not in _SELF_ACTORS for actor in actors)


def _has_affirmative_group_evidence(record: SemanticRecord) -> bool:
    if record.subject_basis == SubjectBasis.REPORT_DEFAULT_GROUP_SCOPE:
        return report_default_group_is_legal(record)
    if (
        record.subject_basis
        == SubjectBasis.NUMERIC_RECONCILIATION_TO_CONSOLIDATED_STATEMENT
    ):
        return bool(record.uncertainty) and any(
            evidence.subject_evidence_pages for evidence in record.evidence
        )
    if record.subject_basis != SubjectBasis.DIRECT_SOURCE_WORDING:
        return False
    source_text = " ".join(
        str(getattr(evidence.anchor, "bounded_quote", "") or "")
        for evidence in record.evidence
    )
    source_text += " " + str(record.source_native.name or "")
    return bool(re.search(r"合并|本集团|集团", source_text))
