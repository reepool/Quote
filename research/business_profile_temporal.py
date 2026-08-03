"""Versioned temporal semantics for governed business-profile record families."""

from __future__ import annotations

import calendar
from dataclasses import asdict, dataclass
from datetime import date
from enum import Enum
from typing import Any


BUSINESS_PROFILE_TEMPORAL_POLICY_SCHEMA_VERSION = "business_profile_temporal_policy.v1"


class BusinessProfileTemporalClass(str, Enum):
    REPORT_FLOW = "report_flow"
    POINT_IN_TIME_STATE = "point_in_time_state"
    EVENT = "event"
    PERSISTENT_RELATIONSHIP = "persistent_relationship"


@dataclass(frozen=True)
class BusinessProfileTemporalPolicy:
    record_type: str
    temporal_class: BusinessProfileTemporalClass
    stable_identity_fields: tuple[str, ...]
    observation_period_field: str | None
    validity_start_field: str | None
    validity_end_field: str | None
    freshness_days: int | None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["temporal_class"] = self.temporal_class.value
        payload["stable_identity_fields"] = list(self.stable_identity_fields)
        return payload


TEMPORAL_POLICIES: tuple[BusinessProfileTemporalPolicy, ...] = (
    BusinessProfileTemporalPolicy(
        "evidence",
        BusinessProfileTemporalClass.EVENT,
        ("instrument_id", "evidence_id"),
        "data_available_date",
        None,
        None,
        None,
    ),
    BusinessProfileTemporalPolicy(
        "events",
        BusinessProfileTemporalClass.EVENT,
        ("instrument_id", "event_type", "event_date", "event_id"),
        "event_date",
        None,
        None,
        None,
    ),
    BusinessProfileTemporalPolicy(
        "regimes",
        BusinessProfileTemporalClass.POINT_IN_TIME_STATE,
        ("instrument_id", "regime_key"),
        None,
        "valid_from",
        "valid_to",
        None,
    ),
    BusinessProfileTemporalPolicy(
        "segments",
        BusinessProfileTemporalClass.REPORT_FLOW,
        ("instrument_id", "segment_id", "segment_type", "consolidation_scope"),
        "report_period",
        "valid_from",
        "valid_to",
        550,
    ),
    BusinessProfileTemporalPolicy(
        "operating_facts",
        BusinessProfileTemporalClass.REPORT_FLOW,
        (
            "instrument_id",
            "fact_type",
            "segment_id",
            "project_id",
            "fact_scope",
            "unit_normalized",
        ),
        "report_period",
        "valid_from",
        "valid_to",
        550,
    ),
    BusinessProfileTemporalPolicy(
        "activities",
        BusinessProfileTemporalClass.POINT_IN_TIME_STATE,
        ("instrument_id", "action", "object_type", "object_id", "segment_id"),
        "report_period",
        "valid_from",
        "valid_to",
        550,
    ),
    BusinessProfileTemporalPolicy(
        "value_chain_roles",
        BusinessProfileTemporalClass.POINT_IN_TIME_STATE,
        ("instrument_id", "segment_id", "role"),
        "report_period",
        "valid_from",
        "valid_to",
        550,
    ),
    BusinessProfileTemporalPolicy(
        "relationships",
        BusinessProfileTemporalClass.PERSISTENT_RELATIONSHIP,
        (
            "instrument_id",
            "relationship_type",
            "counterparty_name_normalized",
            "scope_id",
        ),
        "report_period",
        "valid_from",
        "valid_to",
        550,
    ),
    BusinessProfileTemporalPolicy(
        "exposure_facts",
        BusinessProfileTemporalClass.REPORT_FLOW,
        (
            "instrument_id",
            "activity_id",
            "exposure_fact_type",
            "product_id",
            "segment_id",
            "fact_scope",
        ),
        "report_period",
        "valid_from",
        "valid_to",
        550,
    ),
    BusinessProfileTemporalPolicy(
        "exposure_assumptions",
        BusinessProfileTemporalClass.POINT_IN_TIME_STATE,
        ("instrument_id", "scope_type", "scope_id", "assumption_type"),
        None,
        "effective_from",
        "effective_to",
        None,
    ),
    BusinessProfileTemporalPolicy(
        "exposures",
        BusinessProfileTemporalClass.POINT_IN_TIME_STATE,
        (
            "instrument_id",
            "scope_type",
            "scope_id",
            "commodity_id",
            "exposure_role",
        ),
        "report_period",
        "effective_from",
        "effective_to",
        550,
    ),
)


SUPERSESSION_COLUMNS = {
    "activities": "supersedes_activity_id",
    "relationships": "supersedes_relationship_id",
    "exposure_facts": "supersedes_fact_id",
    "exposure_assumptions": "supersedes_assumption_id",
    "exposures": "supersedes_exposure_id",
}


def get_business_profile_supersession_column(record_type: str) -> str:
    return SUPERSESSION_COLUMNS.get(str(record_type or "").strip(), "supersedes_record_id")


def get_business_profile_temporal_policy(record_type: str) -> BusinessProfileTemporalPolicy:
    normalized = str(record_type or "").strip()
    for policy in TEMPORAL_POLICIES:
        if policy.record_type == normalized:
            return policy
    raise ValueError(f"unsupported business-profile temporal record type: {normalized}")


def business_profile_temporal_policy_manifest() -> dict[str, Any]:
    return {
        "schema_version": BUSINESS_PROFILE_TEMPORAL_POLICY_SCHEMA_VERSION,
        "policies": [policy.to_dict() for policy in TEMPORAL_POLICIES],
    }


def derive_report_observation_interval(
    report_period: str,
    *,
    period_basis: str | None = None,
) -> tuple[str, str]:
    """Return the closed observation interval represented by a report flow."""

    try:
        period_end = date.fromisoformat(str(report_period or "")[:10])
    except ValueError as exc:
        raise ValueError(f"invalid report_period: {report_period}") from exc
    normalized_basis = str(period_basis or "").strip().lower()
    if normalized_basis in {"annual", "annual_report", "fy"} or (
        not normalized_basis and period_end.month == 12
    ):
        period_start = date(period_end.year, 1, 1)
    elif normalized_basis in {"semiannual", "semiannual_report", "h1"} or (
        not normalized_basis and period_end.month == 6
    ):
        period_start = date(period_end.year, 1, 1)
    elif normalized_basis in {"quarter", "quarterly", "q1", "q2", "q3", "q4"}:
        quarter = (period_end.month - 1) // 3
        period_start = date(period_end.year, quarter * 3 + 1, 1)
    else:
        period_start = date(period_end.year, period_end.month, 1)
    last_day = calendar.monthrange(period_end.year, period_end.month)[1]
    if period_end.day != last_day:
        raise ValueError("report_period must be a calendar month end")
    return period_start.isoformat(), period_end.isoformat()
