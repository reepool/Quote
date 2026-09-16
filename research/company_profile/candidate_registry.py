"""Full A-share company-profile candidate registry.

This module composes the existing official-asset universe, coverage rows, and
industry membership reads. It does not apply manufacturing first-wave filters,
fixed report years, or shadow/OOS exclusion lists.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator

from research.announcement_assets.models import timestamp_not_after_as_of

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
    universe_coverage_guarantee: Literal[
        "full_market", "complete_unpaired", "unknown"
    ] = "unknown"
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
ReportLookup = Callable[..., Any]


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
    effective_reports: Mapping[str, Any] | None = None,
    universe_snapshot_id: str | None = None,
    universe_policy_version: str | None = None,
    excluded_instrument_ids: frozenset[str] | None = None,
    allowed_industry_groups: frozenset[str] | None = None,
    universe_coverage_guarantee: Literal[
        "full_market", "complete_unpaired", "unknown"
    ] = "unknown",
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
            as_of=as_of,
        ),
        *_candidates_from_rows(
            indeterminate,
            universe_status="indeterminate",
            coverage=coverage,
            memberships=memberships,
            reports=reports,
            as_of=as_of,
        ),
    ]
    candidates.sort(key=lambda item: item.instrument_id)
    return AShareCandidateRegistry(
        as_of=as_of,
        universe_snapshot_id=universe_snapshot_id,
        universe_policy_version=universe_policy_version,
        universe_coverage_guarantee=universe_coverage_guarantee,
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
    loaded, coverage_guarantee = _resolve_universe_snapshot(
        universe_repository,
        as_of=as_of,
        snapshot=snapshot,
    )

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
        instrument_id: _invoke_report_lookup(
            effective_report_lookup, instrument_id, as_of
        )
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
        universe_coverage_guarantee=coverage_guarantee,
    )


def announcement_access_report_lookup(access: Any) -> ReportLookup:
    """Adapt AnnouncementAssetAccess.get_effective_asset to the registry lookup."""

    def lookup(instrument_id: str, as_of: str) -> Any:
        return access.get_effective_asset(
            instrument_id,
            knowledge_cutoff=_knowledge_cutoff(as_of),
        )

    return lookup


def load_official_task_candidate_registry(
    *,
    as_of: str,
    storage: Any,
    shared_asset_access: Any,
) -> AShareCandidateRegistry:
    """Load the official A-share denominator for the published task owner."""

    if shared_asset_access is None:
        raise ValueError("official live run requires announcement asset access")
    repository = getattr(shared_asset_access, "repository", None)
    if repository is None:
        raise ValueError("official live run requires a universe repository")
    return load_a_share_candidate_registry(
        universe_repository=repository,
        industry_lookup=_storage_industry_lookup(storage),
        effective_report_lookup=announcement_access_report_lookup(shared_asset_access),
        as_of=as_of,
    )


def _storage_industry_lookup(storage: Any) -> IndustryLookup:
    def lookup(instrument_id: str, as_of: str) -> Mapping[str, Any] | None:
        as_of_getter = getattr(storage, "get_industry_membership_as_of", None)
        if callable(as_of_getter):
            row = as_of_getter(instrument_id, as_of)
            if isinstance(row, Mapping):
                return row
        getter = getattr(storage, "get_industry_membership", None)
        if callable(getter):
            row = getter(instrument_id)
            if isinstance(row, Mapping):
                return row
        return None

    return lookup


def _candidates_from_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    universe_status: Literal["eligible", "indeterminate"],
    coverage: Mapping[str, Mapping[str, Any]],
    memberships: Mapping[str, Mapping[str, Any] | None],
    reports: Mapping[str, Any],
    as_of: str,
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
        report = _latest_report(reports.get(instrument_id), as_of=as_of)
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


def _latest_report(row: Any, *, as_of: str) -> LatestAnnualReport | None:
    data = _report_mapping(row)
    if data is None:
        return None
    if _report_visible_after(data, as_of):
        return None
    asset_id = _optional_text(data.get("asset_id"))
    report_period = _optional_text(data.get("report_period"))
    availability = _optional_text(
        data.get("availability") or data.get("asset_availability")
    )
    decision_state = _optional_text(
        data.get("decision_state")
        or data.get("effective_decision_state")
        or data.get("effective_state")
    )
    fiscal_year = data.get("fiscal_year")
    if not asset_id or not report_period or not availability or not decision_state:
        return None
    if isinstance(fiscal_year, bool) or not isinstance(fiscal_year, int):
        return None
    return LatestAnnualReport(
        asset_id=asset_id,
        fiscal_year=fiscal_year,
        report_period=report_period,
        content_hash=_optional_text(data.get("content_hash")),
        availability=availability,
        decision_state=decision_state,
        published_at=_optional_text(data.get("published_at")),
    )


def _report_mapping(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    if isinstance(row, Mapping):
        return dict(row)
    return {
        "asset_id": getattr(row, "asset_id", None),
        "fiscal_year": getattr(row, "fiscal_year", None),
        "report_period": getattr(row, "report_period", None),
        "content_hash": getattr(row, "content_hash", None),
        "availability": _enum_value(getattr(row, "availability", None)),
        "asset_availability": _enum_value(getattr(row, "asset_availability", None)),
        "decision_state": _enum_value(getattr(row, "decision_state", None)),
        "effective_state": getattr(row, "effective_state", None),
        "effective_decision_state": getattr(row, "effective_decision_state", None),
        "published_at": getattr(row, "published_at", None),
        "version_available_at": getattr(row, "version_available_at", None),
        "activated_at": getattr(row, "activated_at", None),
    }


def _report_visible_after(row: Mapping[str, Any], as_of: str) -> bool:
    for key in ("published_at", "version_available_at", "activated_at"):
        value = row.get(key)
        if value and not _timestamp_not_after(str(value), as_of, missing_ok=True):
            return True
    return False


def _resolve_universe_snapshot(
    universe_repository: UniverseSnapshotRepository,
    *,
    as_of: str,
    snapshot: Mapping[str, Any] | None,
) -> tuple[Mapping[str, Any], Literal["full_market", "complete_unpaired", "unknown"]]:
    if snapshot is not None:
        _ensure_snapshot_as_of(snapshot, as_of)
        return snapshot, _caller_coverage_guarantee(snapshot)

    as_of_full = getattr(
        universe_repository, "get_full_market_universe_snapshot_as_of", None
    )
    if callable(as_of_full):
        loaded = as_of_full(as_of)
        if loaded is not None:
            _ensure_snapshot_as_of(loaded, as_of)
            return loaded, "full_market"

    latest_full = universe_repository.get_latest_full_market_universe_snapshot()
    if latest_full is not None and _snapshot_usable_as_of(latest_full, as_of):
        return latest_full, "full_market"

    as_of_complete = getattr(
        universe_repository, "get_complete_universe_snapshot_as_of", None
    )
    if callable(as_of_complete):
        loaded = as_of_complete(as_of)
        if loaded is not None:
            _ensure_snapshot_as_of(loaded, as_of)
            return loaded, "complete_unpaired"

    if latest_full is not None:
        raise ValueError(
            f"no universe snapshot is available as of {as_of}; "
            "refusing to label current data with a historical as_of"
        )

    latest_complete = universe_repository.get_latest_complete_universe_snapshot()
    if latest_complete is not None and _snapshot_usable_as_of(latest_complete, as_of):
        return latest_complete, "complete_unpaired"
    if latest_complete is not None:
        raise ValueError(
            f"no universe snapshot is available as of {as_of}; "
            "refusing to label current data with a historical as_of"
        )
    raise ValueError("universe snapshot is required for the A-share candidate registry")


def _caller_coverage_guarantee(
    snapshot: Mapping[str, Any],
) -> Literal["full_market", "complete_unpaired", "unknown"]:
    if snapshot.get("paired_census_snapshot_id"):
        return "full_market"
    return "unknown"


def _ensure_snapshot_as_of(snapshot: Mapping[str, Any], as_of: str) -> None:
    if not _snapshot_usable_as_of(snapshot, as_of):
        raise ValueError(
            f"no universe snapshot is available as of {as_of}; "
            "refusing to label current data with a historical as_of"
        )


def _snapshot_usable_as_of(snapshot: Mapping[str, Any], as_of: str) -> bool:
    snapshot_at = _optional_text(snapshot.get("snapshot_at"))
    if snapshot_at is None:
        return False
    return _timestamp_not_after(snapshot_at, as_of, missing_ok=False)


def _invoke_report_lookup(lookup: ReportLookup, instrument_id: str, as_of: str) -> Any:
    try:
        parameters = inspect.signature(lookup).parameters
    except (TypeError, ValueError):
        parameters = {}
    if "as_of" in parameters:
        return lookup(instrument_id, as_of=as_of)
    if any(
        item.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
        for item in parameters.values()
    ):
        return lookup(instrument_id, as_of)
    if len(parameters) >= 2:
        return lookup(instrument_id, as_of)
    return lookup(instrument_id)


def _knowledge_cutoff(as_of: str) -> str:
    text = str(as_of or "").strip()
    if len(text) == 10:
        return f"{text}T23:59:59.999999+08:00"
    return text


def _timestamp_not_after(value: str, as_of: str, *, missing_ok: bool) -> bool:
    text = str(value or "").strip()
    bound = str(as_of or "").strip()
    if not text:
        return missing_ok
    if not bound:
        raise ValueError("as_of is required")
    return timestamp_not_after_as_of(text, bound)


def _enum_value(value: Any) -> Any:
    if value is None:
        return None
    return getattr(value, "value", value)


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
