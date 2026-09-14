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
_SUBJECT_CONFLICT = re.compile(r"冲突|不可区分|无法归属|conflict", re.IGNORECASE)

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
    if scope in _NARROWER_SCOPES:
        return candidate
    if (
        _object_type_value(candidate.get("object_type")) == "Segment"
        or candidate.get("row_class") == "consolidation_adjustment"
    ):
        return candidate
    if scope not in (None, "", "unclear"):
        return candidate
    if _draft_has_subject_conflict(candidate) or _draft_is_third_party_activity(
        candidate
    ):
        candidate["subject_scope"] = "unclear"
        return candidate
    candidate["subject_scope"] = "consolidated_group"
    candidate["subject_basis"] = "report_default_group_scope"
    return candidate


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


def _draft_has_subject_conflict(candidate: Mapping[str, Any]) -> bool:
    return any(
        _SUBJECT_CONFLICT.search(str(item))
        for item in candidate.get("uncertainty") or ()
    )


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
        # Explicit research convention after narrower source scopes were ruled out.
        return True
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
