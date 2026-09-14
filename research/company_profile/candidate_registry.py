"""Full A-share company-profile candidate registry.

This module composes the existing official-asset universe, coverage rows, and
industry membership reads. It does not apply manufacturing first-wave filters,
fixed report years, or shadow/OOS exclusion lists.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import PRODUCTION_AUTHORIZATION

CANDIDATE_REGISTRY_SCHEMA_VERSION = "company_profile_a_share_candidate_registry.v1"
PRODUCTION_SCOPE_POLICY = "a_share_full_market.v1"

_ASSET_STATUSES = frozenset(
    {
        "available",
        "confirmed_missing",
        "incomplete",
        "retryable",
        "blocked",
        "not_covered",
    }
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ClassificationInfo(_StrictModel):
    sw_l1_code: str | None = None
    sw_l1_name: str | None = None
    sw_l2_code: str | None = None
    sw_l2_name: str | None = None
    sw_l3_code: str | None = None
    sw_l3_name: str | None = None
    official_industry_code: str | None = None
    taxonomy_system: str | None = None
    mapping_status: str | None = None


class LatestAnnualReport(_StrictModel):
    asset_id: str = Field(min_length=1)
    fiscal_year: int
    report_period: str = Field(min_length=1)
    content_hash: str | None = None
    availability: str = Field(min_length=1)
    decision_state: str = Field(min_length=1)
    published_at: str | None = None


class AShareProfileCandidate(_StrictModel):
    instrument_id: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    company_name: str
    listed_date: str | None = None
    universe_status: Literal["eligible", "indeterminate"]
    exclusion_reason: str | None = None
    classification_status: Literal["present", "missing"]
    classification: ClassificationInfo | None = None
    asset_status: Literal[
        "available",
        "confirmed_missing",
        "incomplete",
        "retryable",
        "blocked",
        "not_covered",
    ]
    asset_blocker: str | None = None
    latest_effective_annual_report: LatestAnnualReport | None = None

    @model_validator(mode="after")
    def _status_invariants(self) -> AShareProfileCandidate:
        if self.universe_status == "eligible" and self.exclusion_reason:
            raise ValueError("eligible candidates cannot carry an exclusion reason")
        if self.universe_status == "indeterminate" and not self.exclusion_reason:
            raise ValueError("indeterminate candidates require an exclusion reason")
        if self.classification_status == "missing" and self.classification is not None:
            raise ValueError("missing classification cannot carry classification fields")
        if self.classification_status == "present" and self.classification is None:
            raise ValueError("present classification requires classification fields")
        return self


class AShareCandidateRegistry(_StrictModel):
    schema_version: Literal["company_profile_a_share_candidate_registry.v1"] = (
        CANDIDATE_REGISTRY_SCHEMA_VERSION
    )
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    policy_version: Literal["a_share_full_market.v1"] = PRODUCTION_SCOPE_POLICY
    as_of: str = Field(min_length=1)
    universe_snapshot_id: str | None = None
    universe_policy_version: str | None = None
    candidates: tuple[AShareProfileCandidate, ...]
    counts: dict[str, int]

    def candidate(self, instrument_id: str) -> AShareProfileCandidate:
        for item in self.candidates:
            if item.instrument_id == instrument_id:
                return item
        raise KeyError(instrument_id)


class UniverseSnapshotRepository(Protocol):
    def get_latest_full_market_universe_snapshot(self) -> Mapping[str, Any] | None: ...

    def get_latest_complete_universe_snapshot(self) -> Mapping[str, Any] | None: ...

    def list_asset_coverage(
        self, universe_snapshot_id: str
    ) -> Sequence[Mapping[str, Any]]: ...


IndustryLookup = Callable[[str, str], Mapping[str, Any] | None]
ReportLookup = Callable[[str], Mapping[str, Any] | None]


def candidate_registry_schema_manifest() -> dict[str, Any]:
    schema = AShareCandidateRegistry.model_json_schema()
    return {
        "schema_version": CANDIDATE_REGISTRY_SCHEMA_VERSION,
        "title": schema.get("title", "AShareCandidateRegistry"),
        "properties": schema.get("properties", {}),
        "schema": schema,
    }


def build_a_share_candidate_registry(
    *,
    as_of: str,
    eligible_instruments: Sequence[Mapping[str, Any]],
    indeterminate: Sequence[Mapping[str, Any]] = (),
    asset_coverage: Mapping[str, Mapping[str, Any]] | None = None,
    industry_memberships: Mapping[str, Mapping[str, Any] | None] | None = None,
    effective_reports: Mapping[str, Mapping[str, Any] | None] | None = None,
    universe_snapshot_id: str | None = None,
    universe_policy_version: str | None = None,
    excluded_instrument_ids: frozenset[str] | None = None,
    allowed_industry_groups: frozenset[str] | None = None,
) -> AShareCandidateRegistry:
    """Assemble the production denominator from already-loaded source rows.

    ``excluded_instrument_ids`` and ``allowed_industry_groups`` are accepted only
    so callers can prove they are ignored. They never shrink the candidate set.
    """

    del excluded_instrument_ids, allowed_industry_groups
    coverage = asset_coverage or {}
    memberships = industry_memberships or {}
    reports = effective_reports or {}
    candidates = [
        *_candidates_from_rows(
            eligible_instruments,
            universe_status="eligible",
            coverage=coverage,
            memberships=memberships,
            reports=reports,
        ),
        *_candidates_from_rows(
            indeterminate,
            universe_status="indeterminate",
            coverage=coverage,
            memberships=memberships,
            reports=reports,
        ),
    ]
    candidates.sort(key=lambda item: item.instrument_id)
    return AShareCandidateRegistry(
        as_of=as_of,
        universe_snapshot_id=universe_snapshot_id,
        universe_policy_version=universe_policy_version,
        candidates=tuple(candidates),
        counts=_counts(candidates),
    )


def load_a_share_candidate_registry(
    *,
    universe_repository: UniverseSnapshotRepository,
    industry_lookup: IndustryLookup,
    effective_report_lookup: ReportLookup,
    as_of: str,
    snapshot: Mapping[str, Any] | None = None,
    first_wave_universe: Callable[..., Any] | None = None,
    shadow_exclusions: Callable[..., Any] | None = None,
) -> AShareCandidateRegistry:
    """Load a registry from existing universe/coverage/industry stores."""

    del first_wave_universe, shadow_exclusions
    loaded = snapshot or universe_repository.get_latest_full_market_universe_snapshot()
    if loaded is None:
        loaded = universe_repository.get_latest_complete_universe_snapshot()
    if loaded is None:
        raise ValueError("universe snapshot is required for the A-share candidate registry")

    snapshot_id = str(loaded.get("snapshot_id") or "").strip() or None
    coverage_rows = (
        universe_repository.list_asset_coverage(snapshot_id) if snapshot_id else ()
    )
    coverage = {
        _instrument_id(row): row
        for row in coverage_rows
        if _instrument_id(row)
    }
    eligible = _snapshot_rows(loaded, "instruments", "instrument_rows")
    indeterminate = _snapshot_rows(loaded, "indeterminate", "indeterminate_rows")
    instrument_ids = [
        _instrument_id(row)
        for row in (*eligible, *indeterminate)
        if _instrument_id(row)
    ]
    memberships = {
        instrument_id: industry_lookup(instrument_id, as_of)
        for instrument_id in instrument_ids
    }
    reports = {
        instrument_id: effective_report_lookup(instrument_id)
        for instrument_id in instrument_ids
    }
    return build_a_share_candidate_registry(
        as_of=as_of,
        eligible_instruments=eligible,
        indeterminate=indeterminate,
        asset_coverage=coverage,
        industry_memberships=memberships,
        effective_reports=reports,
        universe_snapshot_id=snapshot_id,
        universe_policy_version=str(loaded.get("policy_version") or "") or None,
    )


def _candidates_from_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    universe_status: Literal["eligible", "indeterminate"],
    coverage: Mapping[str, Mapping[str, Any]],
    memberships: Mapping[str, Mapping[str, Any] | None],
    reports: Mapping[str, Mapping[str, Any] | None],
) -> list[AShareProfileCandidate]:
    candidates: list[AShareProfileCandidate] = []
    seen: set[str] = set()
    for row in rows:
        instrument_id = _instrument_id(row)
        if not instrument_id or instrument_id in seen:
            continue
        seen.add(instrument_id)
        classification = _classification(memberships.get(instrument_id))
        coverage_row = coverage.get(instrument_id)
        report = _latest_report(reports.get(instrument_id))
        candidates.append(
            AShareProfileCandidate(
                instrument_id=instrument_id,
                exchange=_exchange(row),
                company_name=_company_name(row),
                listed_date=_optional_text(row.get("listed_date")),
                universe_status=universe_status,
                exclusion_reason=(
                    _optional_text(row.get("reason"))
                    if universe_status == "indeterminate"
                    else None
                ),
                classification_status="present" if classification else "missing",
                classification=classification,
                asset_status=_asset_status(coverage_row),
                asset_blocker=_asset_blocker(coverage_row),
                latest_effective_annual_report=report,
            )
        )
    return candidates


def _counts(candidates: Sequence[AShareProfileCandidate]) -> dict[str, int]:
    eligible = sum(item.universe_status == "eligible" for item in candidates)
    indeterminate = sum(item.universe_status == "indeterminate" for item in candidates)
    classification_missing = sum(
        item.classification_status == "missing" for item in candidates
    )
    asset_available = sum(item.asset_status == "available" for item in candidates)
    return {
        "total": len(candidates),
        "eligible": eligible,
        "indeterminate": indeterminate,
        "classification_missing": classification_missing,
        "asset_available": asset_available,
        "asset_not_available": len(candidates) - asset_available,
    }


def _snapshot_rows(
    snapshot: Mapping[str, Any], *keys: str
) -> tuple[Mapping[str, Any], ...]:
    for key in keys:
        raw = snapshot.get(key)
        if raw is None:
            continue
        if isinstance(raw, Mapping):
            items = raw.get("items", raw.get("rows", ()))
        else:
            items = raw
        if isinstance(items, Sequence) and not isinstance(items, (str, bytes)):
            return tuple(item for item in items if isinstance(item, Mapping))
    return ()


def _classification(row: Mapping[str, Any] | None) -> ClassificationInfo | None:
    if not row:
        return None
    info = ClassificationInfo(
        sw_l1_code=_optional_text(row.get("sw_l1_code")),
        sw_l1_name=_optional_text(row.get("sw_l1_name")),
        sw_l2_code=_optional_text(row.get("sw_l2_code")),
        sw_l2_name=_optional_text(row.get("sw_l2_name")),
        sw_l3_code=_optional_text(row.get("sw_l3_code")),
        sw_l3_name=_optional_text(row.get("sw_l3_name")),
        official_industry_code=_optional_text(
            row.get("official_industry_code") or row.get("industry_code")
        ),
        taxonomy_system=_optional_text(row.get("taxonomy_system")),
        mapping_status=_optional_text(row.get("mapping_status")),
    )
    if not any(
        (
            info.sw_l1_code,
            info.sw_l1_name,
            info.sw_l2_code,
            info.sw_l2_name,
            info.sw_l3_code,
            info.sw_l3_name,
            info.official_industry_code,
            info.taxonomy_system,
            info.mapping_status,
        )
    ):
        return None
    return info


def _latest_report(row: Mapping[str, Any] | None) -> LatestAnnualReport | None:
    if not row:
        return None
    asset_id = _optional_text(row.get("asset_id"))
    report_period = _optional_text(row.get("report_period"))
    availability = _optional_text(row.get("availability"))
    decision_state = _optional_text(row.get("decision_state"))
    fiscal_year = row.get("fiscal_year")
    if not asset_id or not report_period or not availability or not decision_state:
        return None
    if not isinstance(fiscal_year, int):
        return None
    return LatestAnnualReport(
        asset_id=asset_id,
        fiscal_year=fiscal_year,
        report_period=report_period,
        content_hash=_optional_text(row.get("content_hash")),
        availability=availability,
        decision_state=decision_state,
        published_at=_optional_text(row.get("published_at")),
    )


def _asset_status(row: Mapping[str, Any] | None) -> str:
    if not row:
        return "not_covered"
    status = str(row.get("status") or "").strip()
    if status in _ASSET_STATUSES:
        return status
    return "not_covered"


def _asset_blocker(row: Mapping[str, Any] | None) -> str | None:
    if not row:
        return None
    evidence = row.get("evidence")
    if isinstance(evidence, Mapping):
        blocker = _optional_text(evidence.get("coverage_blocker") or evidence.get("blocker"))
        if blocker:
            return blocker
    return _optional_text(row.get("coverage_blocker") or row.get("blocker"))


def _instrument_id(row: Mapping[str, Any]) -> str:
    return str(row.get("instrument_id") or "").strip()


def _exchange(row: Mapping[str, Any]) -> str:
    return str(row.get("exchange") or "").strip() or "UNKNOWN"


def _company_name(row: Mapping[str, Any]) -> str:
    listing = row.get("listing_metadata")
    listing_name = ""
    if isinstance(listing, Mapping):
        listing_name = str(listing.get("security_name") or "").strip()
    return str(
        row.get("name")
        or row.get("security_name")
        or listing_name
        or _instrument_id(row)
    ).strip()


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None
