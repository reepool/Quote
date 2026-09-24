"""First-expansion mode and immutable plan for company-profile common-core.

This is the unique owner for company_profile_first_expansion_mode.v1 and
company_profile_first_expansion_plan.v1. It does not add a published action.
Ordinary run/resume stay unconstrained unless the mode is active.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from research.company_profile.candidate_registry import (
    CANDIDATE_REGISTRY_SCHEMA_VERSION,
    AShareCandidateRegistry,
)
from research.company_profile.live_plan import (
    DEFAULT_LIVE_MAX_COMPANIES,
    CompanyProfileLivePlan,
    assign_disclosure_form,
    record_company_profile_live_plan,
)
from research.company_profile.live_run import (
    LIVE_RUN_SCHEMA_VERSION,
    CompanyProfileLiveRunReport,
    FrozenOfficialReportReference,
    persist_live_run_report,
)
from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.operator_closure import (
    CompanyProfileOperatorClosureV2Report,
    persist_operator_closure_v2_report,
    record_operator_closure_v2_report,
)
from research.company_profile.publication import CompanyProfilePublicationControl
from research.company_profile.source_review import (
    SOURCE_REVIEW_SCHEMA_VERSION,
    CompanyProfileSourceReviewReport,
    FixtureGuardResult,
    FreshnessObservation,
    SemanticFinding,
    StructuralCheck,
    persist_source_review_report,
    record_source_review_report,
)

FIRST_EXPANSION_MODE_SCHEMA = "company_profile_first_expansion_mode.v1"
FIRST_EXPANSION_PLAN_SCHEMA = "company_profile_first_expansion_plan.v1"
FirstExpansionMode = Literal["inactive", "active", "completed"]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class FirstExpansionRegistryIdentity(_StrictModel):
    schema_version: Literal["company_profile_a_share_candidate_registry.v1"]
    as_of: str = Field(min_length=1)
    universe_snapshot_id: str | None = None


class FirstExpansionStratum(_StrictModel):
    instrument_id: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    disclosure_form: str = Field(min_length=1)


FirstExpansionReportReference = FrozenOfficialReportReference


class FirstExpansionPlan(_StrictModel):
    schema_version: Literal["company_profile_first_expansion_plan.v1"] = (
        FIRST_EXPANSION_PLAN_SCHEMA
    )
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    plan_id: str = Field(min_length=1)
    knowledge_cutoff: str = Field(min_length=1)
    live_plan: CompanyProfileLivePlan
    registry: FirstExpansionRegistryIdentity
    selected_instrument_ids: tuple[str, ...]
    selected_strata: tuple[FirstExpansionStratum, ...]
    reports: tuple[FirstExpansionReportReference, ...]

    @model_validator(mode="after")
    def _plan_is_frozen_and_contains_service(self) -> FirstExpansionPlan:
        if self.production_authorization != "not_authorized":
            raise ValueError("first expansion cannot authorize production")
        if not self.selected_instrument_ids:
            raise ValueError("first expansion plan must freeze at least one report")
        if self.live_plan.budget.max_companies_this_round != len(
            self.selected_instrument_ids
        ):
            raise ValueError("live-plan budget must equal the frozen report count")
        if tuple(item.instrument_id for item in self.selected_strata) != (
            self.selected_instrument_ids
        ):
            raise ValueError("frozen strata must follow selected instrument ids")
        if tuple(item.instrument_id for item in self.reports) != (
            self.selected_instrument_ids
        ):
            raise ValueError("frozen reports must follow selected instrument ids")
        if len(self.selected_instrument_ids) != DEFAULT_LIVE_MAX_COMPANIES:
            raise ValueError("first expansion must freeze exactly two companies")
        if len(set(self.selected_instrument_ids)) != DEFAULT_LIVE_MAX_COMPANIES:
            raise ValueError("first expansion cannot repeat an instrument")
        if (
            self.live_plan.budget.max_companies_this_round
            != DEFAULT_LIVE_MAX_COMPANIES
        ):
            raise ValueError("first expansion cannot exceed the two-company budget")
        if "service" not in {item.disclosure_form for item in self.selected_strata}:
            raise ValueError("first expansion plan must occupy a service stratum")
        if len({(item.exchange, item.disclosure_form) for item in self.selected_strata}) < 2:
            raise ValueError("first expansion must occupy two different strata")
        if self.registry.as_of != self.knowledge_cutoff:
            raise ValueError("registry as_of must match knowledge_cutoff")
        return self


class FirstExpansionModeState(_StrictModel):
    schema_version: Literal["company_profile_first_expansion_mode.v1"] = (
        FIRST_EXPANSION_MODE_SCHEMA
    )
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    mode: FirstExpansionMode
    plan_id: str | None = None
    work_ids: tuple[str, ...] = ()
    delivered: bool = False

    @model_validator(mode="after")
    def _mode_matches_plan_pointer(self) -> FirstExpansionModeState:
        if self.mode == "inactive" and self.plan_id:
            raise ValueError("inactive first expansion cannot point at a plan")
        if self.mode in {"active", "completed"} and not self.plan_id:
            raise ValueError("active or completed first expansion requires a plan id")
        if self.mode != "active" and self.work_ids:
            raise ValueError("frozen work ids are only remembered while active")
        return self


def load_first_expansion_mode(root: str | Path) -> FirstExpansionModeState:
    """Load the published-owner mode. Missing file means inactive."""

    path = _mode_path(root)
    if not path.is_file():
        return FirstExpansionModeState(mode="inactive")
    return FirstExpansionModeState.model_validate_json(path.read_text(encoding="utf-8"))


def first_expansion_should_constrain_run(root: str | Path) -> bool:
    """Return True only when ordinary run/resume must obey the frozen plan."""

    return load_first_expansion_mode(root).mode == "active"


def load_first_expansion_plan(root: str | Path) -> FirstExpansionPlan | None:
    """Load the frozen plan only when pointer and immutable snapshot match."""

    pointer_path = _plan_pointer_path(root)
    if not pointer_path.is_file():
        return None
    pointer = FirstExpansionPlan.model_validate_json(
        pointer_path.read_text(encoding="utf-8")
    )
    snapshot_path = _plan_snapshot_path(root, pointer.plan_id)
    if not snapshot_path.is_file():
        raise ValueError("active first expansion requires a matching plan snapshot")
    snapshot = FirstExpansionPlan.model_validate_json(
        snapshot_path.read_text(encoding="utf-8")
    )
    if snapshot != pointer:
        raise ValueError("first expansion plan snapshot does not match the pointer")
    return snapshot


def record_first_expansion_plan(
    *,
    registry: AShareCandidateRegistry,
    knowledge_cutoff: str,
    official_bindings: Mapping[str, Mapping[str, Any]] | None = None,
    official_access: Any | None = None,
) -> FirstExpansionPlan:
    """Freeze two companies, including one reserved service seat."""

    cutoff = str(knowledge_cutoff or "").strip()
    if registry.as_of != cutoff:
        raise ValueError("knowledge_cutoff must match the registry as_of used to sample")
    selected = select_first_expansion_targets(registry)
    live_plan = record_company_profile_live_plan(
        max_companies_this_round=DEFAULT_LIVE_MAX_COMPANIES
    )
    resolved_bindings = dict(official_bindings or {})
    if official_access is not None:
        resolved_bindings.update(
            official_bindings_from_shared_access(
                official_access,
                selected,
                knowledge_cutoff=cutoff,
            )
        )
    if official_bindings is None and official_access is None:
        raise ValueError("first expansion requires official asset bindings")
    strata = tuple(
        FirstExpansionStratum(
            instrument_id=instrument_id,
            exchange=registry.candidate(instrument_id).exchange,
            disclosure_form=assign_disclosure_form(registry.candidate(instrument_id)),
        )
        for instrument_id in selected
    )
    reports = tuple(
        _frozen_report(
            registry.candidate(instrument_id),
            resolved_bindings.get(instrument_id),
        )
        for instrument_id in selected
    )
    identity = FirstExpansionRegistryIdentity(
        schema_version=CANDIDATE_REGISTRY_SCHEMA_VERSION,
        as_of=cutoff,
        universe_snapshot_id=registry.universe_snapshot_id,
    )
    plan_id = _plan_id(
        knowledge_cutoff=cutoff,
        registry=identity,
        selected_instrument_ids=selected,
        reports=reports,
    )
    return FirstExpansionPlan(
        plan_id=plan_id,
        knowledge_cutoff=cutoff,
        live_plan=live_plan,
        registry=identity,
        selected_instrument_ids=selected,
        selected_strata=strata,
        reports=reports,
    )


def select_first_expansion_targets(
    registry: AShareCandidateRegistry,
) -> tuple[str, str]:
    """Reserve one service seat, then fill a different stratum from global priority.

    Ordinary live-run sampling is unchanged. This selector is only for the
    first-expansion plan.
    """

    rule = record_company_profile_live_plan(
        max_companies_this_round=DEFAULT_LIVE_MAX_COMPANIES
    ).sampling
    exchange_order = {name: index for index, name in enumerate(rule.exchange_strata)}
    eligible = [
        candidate
        for candidate in registry.candidates
        if candidate.exchange in exchange_order
        and candidate.asset_status == rule.review_eligible_asset_status
        and (
            not rule.review_requires_latest_effective_annual_report
            or candidate.latest_effective_annual_report is not None
        )
    ]
    services = sorted(
        (
            candidate
            for candidate in eligible
            if assign_disclosure_form(candidate) == "service"
        ),
        key=lambda candidate: (
            exchange_order[candidate.exchange],
            candidate.instrument_id,
        ),
    )
    if not services:
        raise ValueError("first expansion cannot activate without a service stratum")
    service = services[0]
    service_key = (service.exchange, "service")
    buckets: dict[tuple[str, str], list[str]] = {
        stratum: [] for stratum in rule.stratum_priority
    }
    for candidate in eligible:
        if candidate.instrument_id == service.instrument_id:
            continue
        key = (candidate.exchange, assign_disclosure_form(candidate))
        if key == service_key or key not in buckets:
            continue
        buckets[key].append(candidate.instrument_id)
    for stratum in rule.stratum_priority:
        if stratum == service_key:
            continue
        members = sorted(set(buckets[stratum]))
        if members:
            return (service.instrument_id, members[0])
    raise ValueError("first expansion cannot activate without a second stratum")


def persist_first_expansion_plan(
    plan: FirstExpansionPlan,
    root: str | Path,
) -> Path:
    """Write the immutable plan snapshot. Same content may be rewritten."""

    payload = plan.model_dump_json(indent=2)
    snapshot = _plan_snapshot_path(root, plan.plan_id)
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    if snapshot.is_file() and snapshot.read_text(encoding="utf-8") != payload:
        raise ValueError("first expansion plan snapshot is immutable")
    pointer = _plan_pointer_path(root)
    if pointer.is_file():
        existing = FirstExpansionPlan.model_validate_json(
            pointer.read_text(encoding="utf-8")
        )
        if existing.plan_id != plan.plan_id:
            raise ValueError("first expansion plan snapshot is immutable")
    tmp = snapshot.with_suffix(".tmp")
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(snapshot)
    pointer_tmp = pointer.with_suffix(".tmp")
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pointer_tmp.write_text(payload, encoding="utf-8")
    pointer_tmp.replace(pointer)
    return snapshot


def activate_first_expansion(
    root: str | Path,
    plan: FirstExpansionPlan,
) -> FirstExpansionModeState:
    """Persist the plan, then mark the published owner active."""

    current = load_first_expansion_mode(root)
    if current.mode == "completed":
        raise ValueError("completed first expansion cannot be activated again")
    persist_first_expansion_plan(plan, root)
    if current.mode == "active" and current.plan_id != plan.plan_id:
        raise ValueError("active first expansion already has a frozen plan")
    state = FirstExpansionModeState(
        mode="active",
        plan_id=plan.plan_id,
        work_ids=current.work_ids if current.plan_id == plan.plan_id else (),
        delivered=current.delivered if current.plan_id == plan.plan_id else False,
    )
    _persist_mode(state, root)
    return state


def remember_first_expansion_work_ids(
    root: str | Path,
    *,
    work_ids: Sequence[str],
) -> FirstExpansionModeState:
    """Remember the frozen work items created by the first active enqueue."""

    current = load_first_expansion_mode(root)
    if current.mode != "active" or not current.plan_id:
        raise ValueError("frozen work ids can only be stored while first expansion is active")
    state = FirstExpansionModeState(
        mode="active",
        plan_id=current.plan_id,
        work_ids=tuple(str(item) for item in work_ids if str(item)),
        delivered=current.delivered,
    )
    _persist_mode(state, root)
    return state


def mark_first_expansion_delivery(root: str | Path) -> FirstExpansionModeState:
    """Mark the frozen sample delivered so later active calls stay idempotent."""

    current = load_first_expansion_mode(root)
    if current.mode != "active" or not current.plan_id:
        raise ValueError("delivery can only be marked while first expansion is active")
    state = FirstExpansionModeState(
        mode="active",
        plan_id=current.plan_id,
        work_ids=current.work_ids,
        delivered=True,
    )
    _persist_mode(state, root)
    return state


def refuse_first_expansion_drift(
    plan: FirstExpansionPlan,
    *,
    knowledge_cutoff: str,
    registry: AShareCandidateRegistry,
    official_bindings: Mapping[str, Mapping[str, Any]],
) -> None:
    """Refuse cutoff, registry, or official-report version drift."""

    if str(knowledge_cutoff or "").strip() != plan.knowledge_cutoff:
        raise ValueError("knowledge_cutoff does not match the frozen first-expansion plan")
    if registry.schema_version != plan.registry.schema_version:
        raise ValueError("registry schema does not match the frozen first-expansion plan")
    if registry.as_of != plan.registry.as_of:
        raise ValueError("registry as_of does not match the frozen first-expansion plan")
    if registry.universe_snapshot_id != plan.registry.universe_snapshot_id:
        raise ValueError(
            "universe_snapshot does not match the frozen first-expansion plan"
        )
    for expected in plan.reports:
        try:
            actual = _frozen_report(
                registry.candidate(expected.instrument_id),
                official_bindings.get(expected.instrument_id),
            )
        except (KeyError, ValueError) as exc:
            raise ValueError(
                "report reference drifted from the frozen first-expansion plan"
            ) from exc
        if actual != expected:
            raise ValueError(
                "report reference drifted from the frozen first-expansion plan"
            )


def persist_first_expansion_live_run(
    report: CompanyProfileLiveRunReport,
    root: str | Path,
    plan: FirstExpansionPlan,
) -> Path:
    """Write a live-run snapshot that does not overwrite the v1 baseline."""

    _require_observation_matches_plan(report, plan)
    path = _expansion_report_path(root, plan.plan_id, f"{LIVE_RUN_SCHEMA_VERSION}.json")
    return persist_live_run_report(report, root, destination=path)


def load_first_expansion_live_run(
    root: str | Path,
    plan: FirstExpansionPlan,
) -> CompanyProfileLiveRunReport | None:
    """Load the this-round live-run snapshot for the frozen plan."""

    path = _expansion_report_path(root, plan.plan_id, f"{LIVE_RUN_SCHEMA_VERSION}.json")
    if not path.is_file():
        return None
    return CompanyProfileLiveRunReport.model_validate_json(
        path.read_text(encoding="utf-8")
    )


def persist_first_expansion_source_review(
    report: CompanyProfileSourceReviewReport,
    root: str | Path,
    plan: FirstExpansionPlan,
) -> Path:
    """Write a source-review snapshot that does not overwrite the v1 baseline."""

    _require_observation_matches_plan(report.live_run, plan)
    path = _expansion_report_path(
        root, plan.plan_id, f"{SOURCE_REVIEW_SCHEMA_VERSION}.json"
    )
    return persist_source_review_report(report, root, destination=path)


def load_first_expansion_source_review(
    root: str | Path,
    plan: FirstExpansionPlan,
) -> CompanyProfileSourceReviewReport | None:
    """Load the this-round source-review snapshot for the frozen plan."""

    path = _expansion_report_path(
        root, plan.plan_id, f"{SOURCE_REVIEW_SCHEMA_VERSION}.json"
    )
    if not path.is_file():
        return None
    return CompanyProfileSourceReviewReport.model_validate_json(
        path.read_text(encoding="utf-8")
    )


def record_first_expansion_source_review(
    root: str | Path,
    *,
    structural_checks: Sequence[StructuralCheck] = (),
    semantic_findings: Sequence[SemanticFinding] = (),
    fixture_guards: Sequence[FixtureGuardResult] = (),
    freshness: Sequence[FreshnessObservation] = (),
    tokens_used: int | None = None,
    elapsed_seconds: float | None = None,
    human_review_minutes: float | None = None,
) -> CompanyProfileSourceReviewReport:
    """Record this-round source review against the expansion live-run snapshot."""

    plan = load_first_expansion_plan(root)
    if plan is None:
        raise ValueError("first-expansion source review requires the frozen plan")
    live_run = load_first_expansion_live_run(root, plan)
    if live_run is None:
        raise ValueError("first-expansion source review requires the new live-run snapshot")
    report = record_source_review_report(
        live_run=live_run,
        structural_checks=structural_checks,
        semantic_findings=semantic_findings,
        fixture_guards=fixture_guards,
        freshness=freshness,
        tokens_used=tokens_used,
        elapsed_seconds=elapsed_seconds,
        human_review_minutes=human_review_minutes,
    )
    persist_first_expansion_source_review(report, root, plan)
    return report


def official_bindings_from_shared_access(
    access: Any,
    instrument_ids: Sequence[str],
    *,
    knowledge_cutoff: str,
) -> dict[str, dict[str, str]]:
    """Resolve official report_id and document_version from existing asset bindings."""

    if access is None:
        raise ValueError("first expansion requires official asset bindings")
    getter = getattr(access, "get_effective_asset", None)
    if not callable(getter):
        raise ValueError("first expansion requires official asset bindings")
    bindings: dict[str, dict[str, str]] = {}
    for instrument_id in instrument_ids:
        asset = getter(instrument_id, knowledge_cutoff=knowledge_cutoff)
        if not isinstance(asset, Mapping):
            raise ValueError("first expansion requires an official report_id")
        report_id = (
            str(asset.get("source_asset_id") or "").strip()
            or str(asset.get("filing_id") or "").strip()
            or str(asset.get("source_announcement_id") or "").strip()
        )
        document_version = str(asset.get("content_hash") or "").strip()
        if not report_id:
            raise ValueError("first expansion requires an official report_id")
        if not document_version or document_version.lower() == "unknown":
            raise ValueError("first expansion cannot use unknown document_version")
        asset_id = str(asset.get("asset_id") or "").strip()
        report_period = str(asset.get("report_period") or "").strip()
        if not asset_id or not report_period:
            raise ValueError("first expansion requires official asset_id and report_period")
        bindings[str(instrument_id)] = {
            "asset_id": asset_id,
            "report_id": report_id,
            "report_period": report_period,
            "filing_id": str(asset.get("filing_id") or "").strip(),
            "source_asset_id": str(asset.get("source_asset_id") or "").strip(),
            "source_announcement_id": str(
                asset.get("source_announcement_id") or ""
            ).strip(),
            "document_version": document_version,
        }
    return bindings


def complete_first_expansion(
    root: str | Path,
    *,
    publication: CompanyProfilePublicationControl,
) -> CompanyProfileOperatorClosureV2Report:
    """Write operator-closure v2 after the new review exists and mark completed."""

    current = load_first_expansion_mode(root)
    if current.mode != "active" or not current.plan_id:
        raise ValueError("operator closure v2 requires an active first-expansion plan")
    plan = load_first_expansion_plan(root)
    if plan is None or plan.plan_id != current.plan_id:
        raise ValueError("operator closure v2 requires the frozen first-expansion plan")
    live_run = load_first_expansion_live_run(root, plan)
    review = load_first_expansion_source_review(root, plan)
    if live_run is None or review is None:
        raise ValueError("operator closure v2 requires the new source-review snapshot")
    _require_observation_matches_plan(live_run, plan)
    _require_observation_matches_plan(review.live_run, plan)
    if not live_run.company_outcomes or not all(
        item.delivered for item in live_run.company_outcomes
    ):
        raise ValueError("operator closure v2 requires the frozen sample to be delivered")
    reviewed = {item.instrument_id for item in review.semantic_findings}
    if reviewed != set(plan.selected_instrument_ids):
        raise ValueError("operator closure v2 must cover the frozen sample")
    if (
        review.source_recall.status != "assessed"
        or review.source_accuracy.status != "assessed"
        or review.critical_numeric_errors.status != "assessed"
    ):
        raise ValueError(
            "operator closure v2 cannot complete from an unassessed source review"
        )
    report = record_operator_closure_v2_report(publication)
    persist_operator_closure_v2_report(report, root)
    _persist_mode(
        FirstExpansionModeState(mode="completed", plan_id=plan.plan_id),
        root,
    )
    return report


def _frozen_report(
    candidate: Any,
    binding: Mapping[str, Any] | None,
) -> FirstExpansionReportReference:
    report = getattr(candidate, "latest_effective_annual_report", None)
    if report is None:
        raise ValueError("first expansion cannot freeze a name without an official report")
    document_version = ""
    if binding is not None:
        document_version = str(binding.get("document_version") or "").strip()
        if document_version.lower() == "unknown":
            raise ValueError("first expansion cannot use unknown document_version")
    if not document_version:
        document_version = str(getattr(report, "content_hash", "") or "").strip()
    if not document_version or document_version.lower() == "unknown":
        raise ValueError("first expansion cannot use unknown document_version")
    report_id = ""
    if binding is not None:
        report_id = (
            str(binding.get("report_id") or "").strip()
            or str(binding.get("source_asset_id") or "").strip()
            or str(binding.get("filing_id") or "").strip()
        )
    if not report_id:
        raise ValueError("first expansion requires an official report_id")
    asset_id = ""
    report_period = ""
    if binding is not None:
        asset_id = str(binding.get("asset_id") or "").strip()
        report_period = str(binding.get("report_period") or "").strip()
    if not asset_id:
        asset_id = str(getattr(report, "asset_id", "") or "").strip()
    if not report_period:
        report_period = str(getattr(report, "report_period", "") or "").strip()
    if not asset_id or not report_period:
        raise ValueError("first expansion requires official asset_id and report_period")
    return FirstExpansionReportReference(
        instrument_id=str(candidate.instrument_id),
        asset_id=asset_id,
        report_id=report_id,
        report_period=report_period,
        document_version=document_version,
    )


def _require_observation_matches_plan(
    report: CompanyProfileLiveRunReport,
    plan: FirstExpansionPlan,
) -> None:
    if report.first_expansion_plan_id != plan.plan_id:
        raise ValueError("first-expansion live run must carry the frozen plan id")
    if report.frozen_report_references != plan.reports:
        raise ValueError(
            "first-expansion observation must match the frozen report references"
        )


def _plan_id(
    *,
    knowledge_cutoff: str,
    registry: FirstExpansionRegistryIdentity,
    selected_instrument_ids: Sequence[str],
    reports: Sequence[FirstExpansionReportReference],
) -> str:
    payload = {
        "knowledge_cutoff": knowledge_cutoff,
        "registry": registry.model_dump(mode="json"),
        "selected_instrument_ids": list(selected_instrument_ids),
        "reports": [item.model_dump(mode="json") for item in reports],
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )
    ).hexdigest()
    return digest[:32]


def _persist_mode(state: FirstExpansionModeState, root: str | Path) -> Path:
    path = _mode_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(state.model_dump_json(indent=2), encoding="utf-8")
    tmp.replace(path)
    return path


def _mode_path(root: str | Path) -> Path:
    return Path(root) / "reports" / f"{FIRST_EXPANSION_MODE_SCHEMA}.json"


def _plan_pointer_path(root: str | Path) -> Path:
    return Path(root) / "reports" / f"{FIRST_EXPANSION_PLAN_SCHEMA}.json"


def _plan_snapshot_path(root: str | Path, plan_id: str) -> Path:
    return Path(root) / "reports" / "first_expansion" / f"{FIRST_EXPANSION_PLAN_SCHEMA}.{plan_id}.json"


def _expansion_report_path(root: str | Path, plan_id: str, filename: str) -> Path:
    return Path(root) / "reports" / "first_expansion" / plan_id / filename
