"""Frozen-plan consumption for the next M4 small batch.

The existing task owner calls this module. It does not add a published action
and it does not rewrite the completed first-expansion snapshots.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

SHARED_TOKEN_BUDGET = 50_000
_BATCH_DIRNAME = "m4_next_small_batch"
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
    token_budget: int = SHARED_TOKEN_BUDGET
    reports: tuple[M4NextBatchReport, ...]

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


def load_m4_next_batch_plan(root: str | Path) -> M4NextBatchPlan | None:
    """Load the single frozen plan for this round, if one has been written."""

    parent = Path(root) / "reports" / _BATCH_DIRNAME
    if not parent.is_dir():
        return None
    plans = sorted(parent.glob("*/plan.json"))
    if not plans:
        return None
    if len(plans) != 1:
        raise ValueError("m4 next batch requires exactly one frozen plan")
    return M4NextBatchPlan.model_validate_json(plans[0].read_text(encoding="utf-8"))


def save_m4_next_batch_plan(root: str | Path, plan: M4NextBatchPlan) -> Path:
    path = batch_directory(root, plan.plan_id) / "plan.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(plan.model_dump_json(indent=2), encoding="utf-8")
    return path


def load_m4_next_batch_observation(
    root: str | Path,
    plan: M4NextBatchPlan,
) -> M4NextBatchObservation:
    path = batch_directory(root, plan.plan_id) / "observation.json"
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
) -> Path:
    path = batch_directory(root, observation.plan_id) / "observation.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(observation.model_dump_json(indent=2), encoding="utf-8")
    return path


def load_m4_next_batch_observation_for_source_review(
    root: str | Path,
) -> M4NextBatchObservation:
    """Source review reads the merged round, not one call's securities."""

    plan = load_m4_next_batch_plan(root)
    if plan is None:
        raise ValueError("m4 next-batch source review requires the frozen plan")
    observation = load_m4_next_batch_observation(root, plan)
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
