"""Small, program-owned acceptance rules for research-only company profiles.

The policy deliberately reuses the semantic model's existing subject and evidence
fields.  It is not a confidence service or a persisted authorization layer.
"""

from __future__ import annotations

import re
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


def _has_affirmative_group_evidence(record: SemanticRecord) -> bool:
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
