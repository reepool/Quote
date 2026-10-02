"""Frozen-plan consumption for the next M4 small batch.

The existing task owner calls this module. It does not add a published action
and it does not rewrite the completed first-expansion snapshots.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

SHARED_TOKEN_BUDGET = 50_000
MAX_COMPANIES_THIS_ROUND = 2
_BATCH_DIRNAME = "m4_next_small_batch"
_EXCHANGE_ORDER = ("SSE", "SZSE", "BSE")
_DISCLOSURE_FORMS = ("service", "manufacturing")
HISTORICAL_COMPLETED_INSTRUMENTS = frozenset({"600004.SH", "600006.SH"})
_REFERENCE_FIELDS = (
    "asset_id",
    "report_id",
    "report_period",
    "document_version",
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class M4NextBatchReport(_StrictModel):
    instrument_id: str = Field(min_length=1)
    asset_id: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    report_period: str = Field(min_length=1)
    document_version: str = Field(min_length=1)
    disclosure_form: str = Field(min_length=1)


class M4NextBatchPlan(_StrictModel):
    plan_id: str = Field(min_length=1)
    knowledge_cutoff: str = Field(min_length=1)
    max_companies_this_round: int = MAX_COMPANIES_THIS_ROUND
    token_budget: int = SHARED_TOKEN_BUDGET
    reports: tuple[M4NextBatchReport, ...]

    @model_validator(mode="after")
    def _freeze_two_company_budget(self) -> M4NextBatchPlan:
        if self.max_companies_this_round != MAX_COMPANIES_THIS_ROUND:
            raise ValueError("m4 next batch is limited to two companies")
        if self.token_budget != SHARED_TOKEN_BUDGET:
            raise ValueError("m4 next batch shares one 50000 token budget")
        if len(self.reports) != MAX_COMPANIES_THIS_ROUND:
            raise ValueError("m4 next batch plan must name exactly two reports")
        forms = [report.disclosure_form for report in self.reports]
        if forms != list(_DISCLOSURE_FORMS):
            raise ValueError("m4 next batch plan must list service then manufacturing")
        return self

    def report_for(self, instrument_id: str) -> M4NextBatchReport | None:
        for report in self.reports:
            if report.instrument_id == instrument_id:
                return report
        return None


class M4NextBatchOutcome(_StrictModel):
    instrument_id: str = Field(min_length=1)
    status: str = Field(min_length=1)
    tokens_consumed: int = 0
    reused_scope: bool = False
    reason: str | None = None


class M4NextBatchObservation(_StrictModel):
    plan_id: str = Field(min_length=1)
    knowledge_cutoff: str = Field(min_length=1)
    token_budget: int = SHARED_TOKEN_BUDGET
    tokens_consumed: int = 0
    outcomes: tuple[M4NextBatchOutcome, ...] = ()

    def outcome_for(self, instrument_id: str) -> M4NextBatchOutcome | None:
        for outcome in self.outcomes:
            if outcome.instrument_id == instrument_id:
                return outcome
        return None


def batch_directory(root: str | Path, plan_id: str) -> Path:
    return Path(root) / "reports" / _BATCH_DIRNAME / plan_id


def m4_snapshot_directory(
    root: str | Path,
    plan_id: str,
    *,
    plan_directory: str | Path | None = None,
) -> Path:
    """Old rounds stay under the unique batch directory. A repair names its own."""

    if plan_directory is not None:
        return Path(plan_directory)
    return batch_directory(root, plan_id)


def load_m4_next_batch_plan(
    root: str | Path,
    *,
    plan_directory: str | Path | None = None,
) -> M4NextBatchPlan | None:
    """Load the single frozen plan for this round, if one has been written."""

    if plan_directory is not None:
        path = Path(plan_directory) / "plan.json"
        if not path.is_file():
            return None
        return M4NextBatchPlan.model_validate_json(path.read_text(encoding="utf-8"))
    parent = Path(root) / "reports" / _BATCH_DIRNAME
    if not parent.is_dir():
        return None
    plans = sorted(parent.glob("*/plan.json"))
    if not plans:
        return None
    if len(plans) != 1:
        raise ValueError("m4 next batch requires exactly one frozen plan")
    return M4NextBatchPlan.model_validate_json(plans[0].read_text(encoding="utf-8"))


def save_m4_next_batch_plan(
    root: str | Path,
    plan: M4NextBatchPlan,
    *,
    plan_directory: str | Path | None = None,
) -> Path:
    """Write the plan once. A different sample or version is refused."""

    existing = load_m4_next_batch_plan(root, plan_directory=plan_directory)
    if existing is not None:
        if existing != plan:
            raise ValueError("frozen m4 next batch plan cannot be replaced")
        return m4_snapshot_directory(
            root, existing.plan_id, plan_directory=plan_directory
        ) / "plan.json"
    path = m4_snapshot_directory(
        root, plan.plan_id, plan_directory=plan_directory
    ) / "plan.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(plan.model_dump_json(indent=2) + "\n", encoding="utf-8")
    return path


def load_m4_next_batch_observation(
    root: str | Path,
    plan: M4NextBatchPlan,
    *,
    plan_directory: str | Path | None = None,
) -> M4NextBatchObservation:
    path = m4_snapshot_directory(
        root, plan.plan_id, plan_directory=plan_directory
    ) / "observation.json"
    if not path.is_file():
        return M4NextBatchObservation(
            plan_id=plan.plan_id,
            knowledge_cutoff=plan.knowledge_cutoff,
            token_budget=plan.token_budget,
        )
    return M4NextBatchObservation.model_validate_json(path.read_text(encoding="utf-8"))


def save_m4_next_batch_observation(
    root: str | Path,
    observation: M4NextBatchObservation,
    *,
    plan_directory: str | Path | None = None,
) -> Path:
    path = m4_snapshot_directory(
        root, observation.plan_id, plan_directory=plan_directory
    ) / "observation.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(observation.model_dump_json(indent=2), encoding="utf-8")
    return path


def load_m4_next_batch_observation_for_source_review(
    root: str | Path,
    *,
    plan_directory: str | Path | None = None,
) -> M4NextBatchObservation:
    """Source review reads the merged round, not one call's securities."""

    plan = load_m4_next_batch_plan(root, plan_directory=plan_directory)
    if plan is None:
        raise ValueError("m4 next-batch source review requires the frozen plan")
    observation = load_m4_next_batch_observation(
        root, plan, plan_directory=plan_directory
    )
    missing = [
        report.instrument_id
        for report in plan.reports
        if observation.outcome_for(report.instrument_id) is None
    ]
    if missing:
        raise ValueError(
            "m4 next-batch source review requires both company outcomes: "
            + ", ".join(missing)
        )
    return observation


def remaining_token_budget(observation: M4NextBatchObservation) -> int:
    return max(0, int(observation.token_budget) - int(observation.tokens_consumed))


def stable_plan_id(
    *,
    knowledge_cutoff: str,
    reports: Sequence[M4NextBatchReport],
) -> str:
    payload = {
        "knowledge_cutoff": knowledge_cutoff,
        "max_companies_this_round": MAX_COMPANIES_THIS_ROUND,
        "token_budget": SHARED_TOKEN_BUDGET,
        "reports": [report.model_dump() for report in reports],
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def delivered_instrument_ids(
    output_root: str | Path,
    identity: Mapping[str, Any],
) -> set[str]:
    """Instruments that already have a runtime record for this processing identity."""

    namespace = Path(output_root) / "company_profile_common_core.v1"
    found: set[str] = set()
    if not namespace.is_dir():
        return found
    for path in namespace.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, Mapping):
            continue
        execution = payload.get("execution") or {}
        input_identity = execution.get("input_identity") or {}
        processing = input_identity.get("processing_identity") or {}
        if dict(processing) != dict(identity):
            continue
        instrument_id = str((payload.get("report") or {}).get("instrument_id") or "")
        if instrument_id:
            found.add(instrument_id)
    return found


def select_m4_next_batch_reports(
    candidates: Sequence[Any],
    *,
    delivered_instrument_ids: set[str],
    readable,
    binding_for: Callable[[str], Mapping[str, Any] | None],
) -> tuple[M4NextBatchReport, M4NextBatchReport]:
    """Pick the first legal service company, then the first legal manufacturer."""

    excluded = set(delivered_instrument_ids) | HISTORICAL_COMPLETED_INSTRUMENTS
    chosen: list[M4NextBatchReport] = []
    for form in _DISCLOSURE_FORMS:
        report = _first_legal_report(
            candidates,
            disclosure_form=form,
            excluded=excluded,
            readable=readable,
            binding_for=binding_for,
        )
        if report is None:
            raise ValueError(f"m4 next batch has no legal {form} candidate")
        chosen.append(report)
        excluded.add(report.instrument_id)
    return tuple(chosen)


def build_m4_next_batch_plan(
    candidates: Sequence[Any],
    *,
    knowledge_cutoff: str,
    delivered_instrument_ids: set[str],
    readable,
    binding_for: Callable[[str], Mapping[str, Any] | None],
) -> M4NextBatchPlan:
    reports = select_m4_next_batch_reports(
        candidates,
        delivered_instrument_ids=delivered_instrument_ids,
        readable=readable,
        binding_for=binding_for,
    )
    return M4NextBatchPlan(
        plan_id=stable_plan_id(knowledge_cutoff=knowledge_cutoff, reports=reports),
        knowledge_cutoff=knowledge_cutoff,
        max_companies_this_round=MAX_COMPANIES_THIS_ROUND,
        token_budget=SHARED_TOKEN_BUDGET,
        reports=reports,
    )


def _first_legal_report(
    candidates: Sequence[Any],
    *,
    disclosure_form: str,
    excluded: set[str],
    readable,
    binding_for: Callable[[str], Mapping[str, Any] | None],
) -> M4NextBatchReport | None:
    from research.company_profile.live_plan import assign_disclosure_form

    grouped: dict[str, list[Any]] = {exchange: [] for exchange in _EXCHANGE_ORDER}
    for candidate in candidates:
        instrument_id = str(getattr(candidate, "instrument_id", "") or "")
        exchange = str(getattr(candidate, "exchange", "") or "")
        if not instrument_id or exchange not in grouped:
            continue
        if getattr(candidate, "universe_status", None) != "eligible":
            continue
        if instrument_id in excluded:
            continue
        if assign_disclosure_form(candidate) != disclosure_form:
            continue
        if not readable(candidate):
            continue
        grouped[exchange].append(candidate)
    for exchange in _EXCHANGE_ORDER:
        for candidate in sorted(grouped[exchange], key=lambda item: item.instrument_id):
            binding = binding_for(candidate.instrument_id)
            if binding is None:
                continue
            values = {
                field: str(binding.get(field) or "").strip() for field in _REFERENCE_FIELDS
            }
            if any(not values[field] for field in _REFERENCE_FIELDS):
                continue
            return M4NextBatchReport(
                instrument_id=candidate.instrument_id,
                disclosure_form=disclosure_form,
                **values,
            )
    return None


def tokens_consumed_by_call(*, tokens_used: int, reused_scope: bool) -> int:
    """Completed-scope reuse adds nothing. A failed call keeps its tokens."""

    if reused_scope:
        return 0
    return max(0, int(tokens_used))


def drift_reason(
    plan: M4NextBatchPlan,
    report: M4NextBatchReport,
    *,
    knowledge_cutoff: str,
    binding: Mapping[str, Any] | None,
) -> str | None:
    if knowledge_cutoff != plan.knowledge_cutoff:
        return "cutoff_drift"
    if binding is None:
        return "missing_official_binding"
    for field in _REFERENCE_FIELDS:
        if str(binding.get(field) or "").strip() != getattr(report, field):
            return f"{field}_drift"
    return None


def merge_outcome(
    observation: M4NextBatchObservation,
    outcome: M4NextBatchOutcome,
) -> M4NextBatchObservation:
    """Keep every other company and add this call's tokens to that company."""

    previous = observation.outcome_for(outcome.instrument_id)
    tokens = outcome.tokens_consumed
    if previous is not None:
        tokens = previous.tokens_consumed + outcome.tokens_consumed
    stored = outcome.model_copy(update={"tokens_consumed": tokens})
    kept = tuple(
        item for item in observation.outcomes if item.instrument_id != outcome.instrument_id
    )
    outcomes = kept + (stored,)
    return observation.model_copy(
        update={
            "outcomes": outcomes,
            "tokens_consumed": sum(item.tokens_consumed for item in outcomes),
        }
    )


def historical_snapshot_paths(root: str | Path) -> tuple[Path, ...]:
    reports = Path(root) / "reports"
    names = (
        "company_profile_first_expansion_mode.v1.json",
        "company_profile_first_expansion_plan.v1.json",
        "company_profile_operator_closure.v2.json",
        "company_profile_live_run.v1.json",
        "company_profile_source_review.v1.json",
    )
    return tuple(reports / name for name in names)


def snapshot_bytes(paths: Sequence[Path]) -> dict[str, bytes | None]:
    found: dict[str, bytes | None] = {}
    for path in paths:
        found[str(path)] = path.read_bytes() if path.is_file() else None
    return found
