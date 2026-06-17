"""Modular instrument master governance orchestration.

This module contains the policy-neutral contracts and dispatcher for master
data governance. Market-specific evidence and write rules stay in DataManager's
existing sync methods; policies here only adapt those methods to a common
request/result shape.
"""

from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Protocol, Sequence

from utils.date_utils import get_shanghai_time


logger = logging.getLogger(__name__)

SUPPORTED_MODES_BY_SCOPE = {
    "a_share_stock": {"force_refresh", "freshness_gated", "audit_only", "skip_for_backfill"},
    "a_share_index": {"force_refresh", "freshness_gated", "audit_only", "skip_for_backfill"},
    "hkex_instrument": {"audit_only", "safe_write", "lifecycle_write", "skip_for_backfill"},
}

SCOPE_EXCHANGES = {
    "a_share_stock": {"SSE", "SZSE", "BSE"},
    "a_share_index": {"SSE", "SZSE"},
    "hkex_instrument": {"HKEX"},
}

SCOPE_INSTRUMENT_TYPES = {
    "a_share_stock": {"stock"},
    "a_share_index": {"index"},
    "hkex_instrument": {"stock"},
}


@dataclass(frozen=True)
class MasterGovernanceRequirement:
    """One explicit master-governance prerequisite for a business job."""

    scope: str
    exchanges: List[str] = field(default_factory=list)
    instrument_types: List[str] = field(default_factory=list)
    mode: str = "freshness_gated"
    target_date: Optional[date] = None
    job_name: str = "unknown"
    job_type: str = "current"
    continue_on_error: bool = True
    timeout_sec: Optional[int] = None
    freshness_threshold_hours: Optional[float] = None
    include_pytdx_validation: Optional[bool] = None
    legacy_fallback: bool = False
    options: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_config(
        cls,
        raw: Dict[str, Any],
        *,
        job_name: str,
        job_type: str = "current",
        target_date: Optional[date] = None,
        default_continue_on_error: bool = True,
    ) -> "MasterGovernanceRequirement":
        """Build a requirement from config and runtime job context."""
        if not isinstance(raw, dict):
            raise ValueError("master governance requirement must be a mapping")

        known = {
            "scope",
            "exchanges",
            "instrument_types",
            "mode",
            "target_date",
            "job_name",
            "job_type",
            "continue_on_error",
            "timeout_sec",
            "freshness_threshold_hours",
            "include_pytdx_validation",
            "legacy_fallback",
            "options",
        }
        unknown = sorted(set(raw) - known)
        if unknown:
            raise ValueError(f"unknown master governance requirement keys: {unknown}")

        raw_target = raw.get("target_date", target_date)
        normalized_target: Optional[date]
        if isinstance(raw_target, datetime):
            normalized_target = raw_target.date()
        elif isinstance(raw_target, date):
            normalized_target = raw_target
        elif raw_target:
            normalized_target = datetime.fromisoformat(str(raw_target)).date()
        else:
            normalized_target = None

        return cls(
            scope=str(raw.get("scope") or "").strip(),
            exchanges=_normalize_strings(raw.get("exchanges") or []),
            instrument_types=[
                str(value).strip().lower()
                for value in (raw.get("instrument_types") or [])
                if str(value).strip()
            ],
            mode=str(raw.get("mode") or "freshness_gated").strip(),
            target_date=normalized_target,
            job_name=str(raw.get("job_name") or job_name),
            job_type=str(raw.get("job_type") or job_type),
            continue_on_error=bool(raw.get("continue_on_error", default_continue_on_error)),
            timeout_sec=_optional_int(raw.get("timeout_sec")),
            freshness_threshold_hours=_optional_float(raw.get("freshness_threshold_hours")),
            include_pytdx_validation=_optional_bool(raw.get("include_pytdx_validation")),
            legacy_fallback=bool(raw.get("legacy_fallback", False)),
            options=dict(raw.get("options") or {}),
        )

    def key(self) -> tuple:
        return (
            self.scope,
            tuple(self.exchanges),
            tuple(self.instrument_types),
            self.mode,
            self.job_name,
            self.job_type,
        )

    def validate(self) -> None:
        if self.scope not in SCOPE_EXCHANGES:
            raise ValueError(f"unsupported master governance scope: {self.scope}")
        supported_modes = SUPPORTED_MODES_BY_SCOPE.get(self.scope, set())
        if self.mode not in supported_modes:
            raise ValueError(f"unsupported master governance mode for {self.scope}: {self.mode}")

        supported_exchanges = SCOPE_EXCHANGES[self.scope]
        unsupported_exchanges = sorted(set(self.exchanges) - supported_exchanges)
        if unsupported_exchanges:
            raise ValueError(
                f"unsupported exchanges for {self.scope}: {unsupported_exchanges}"
            )

        supported_types = SCOPE_INSTRUMENT_TYPES[self.scope]
        unsupported_types = sorted(set(self.instrument_types or supported_types) - supported_types)
        if unsupported_types:
            raise ValueError(
                f"unsupported instrument types for {self.scope}: {unsupported_types}"
            )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "scope": self.scope,
            "exchanges": list(self.exchanges),
            "instrument_types": list(self.instrument_types),
            "mode": self.mode,
            "target_date": self.target_date.isoformat() if self.target_date else None,
            "job_name": self.job_name,
            "job_type": self.job_type,
            "continue_on_error": self.continue_on_error,
            "timeout_sec": self.timeout_sec,
            "freshness_threshold_hours": self.freshness_threshold_hours,
            "include_pytdx_validation": self.include_pytdx_validation,
            "legacy_fallback": self.legacy_fallback,
            "options": dict(self.options),
        }


class GovernancePolicy(Protocol):
    scope: str

    async def execute(self, requirement: MasterGovernanceRequirement) -> Dict[str, Any]:
        ...


class PolicyRegistry:
    """Simple policy registry with config-driven enablement."""

    def __init__(self, policies: Optional[Iterable[GovernancePolicy]] = None):
        self._policies: Dict[str, GovernancePolicy] = {}
        for policy in policies or []:
            self.register(policy)

    def register(self, policy: GovernancePolicy) -> None:
        self._policies[policy.scope] = policy

    def get(self, scope: str) -> Optional[GovernancePolicy]:
        return self._policies.get(scope)


class MasterGovernanceOrchestrator:
    """Dispatch explicit governance requirements and merge policy results."""

    def __init__(
        self,
        *,
        registry: PolicyRegistry,
        policy_config: Optional[Dict[str, Any]] = None,
    ):
        self.registry = registry
        self.policy_config = policy_config or {}

    async def run(
        self,
        requirements: Sequence[MasterGovernanceRequirement],
    ) -> Dict[str, Any]:
        started_at = get_shanghai_time()
        children: List[Dict[str, Any]] = []
        warnings: List[str] = []
        errors: List[str] = []

        try:
            requirements = validate_requirements(requirements)
        except ValueError as exc:
            return _base_result(
                status="error",
                action="validation_failed",
                reason="invalid_master_governance_requirements",
                started_at=started_at,
                children=[],
                warnings=[],
                errors=[str(exc)],
            )

        if not requirements:
            return _base_result(
                status="skipped",
                action="skipped",
                reason="no_master_governance_requirements",
                started_at=started_at,
                children=[],
                warnings=[],
                errors=[],
            )

        for requirement in requirements:
            policy_enabled = (
                self.policy_config.get(requirement.scope, {}).get("enabled", True)
                if isinstance(self.policy_config.get(requirement.scope), dict)
                else True
            )
            if not policy_enabled:
                child = _child_result(
                    requirement,
                    status="skipped",
                    action="skipped",
                    reason="policy_disabled_by_config",
                )
            elif requirement.mode == "skip_for_backfill":
                child = _child_result(
                    requirement,
                    status="skipped",
                    action="skipped",
                    reason="historical_current_master_governance_skipped",
                )
            else:
                policy = self.registry.get(requirement.scope)
                if policy is None:
                    child = _child_result(
                        requirement,
                        status="error" if not requirement.continue_on_error else "warning",
                        action="unsupported",
                        reason="unsupported_master_governance_policy",
                        errors=[f"unsupported master governance policy: {requirement.scope}"],
                    )
                else:
                    try:
                        child = await policy.execute(requirement)
                    except Exception as exc:
                        if not requirement.continue_on_error:
                            raise
                        child = _child_result(
                            requirement,
                            status="error",
                            action="failed",
                            reason="policy_execution_failed",
                            errors=[str(exc)],
                        )
            child = _normalize_child_result(requirement, child)
            children.append(child)
            warnings.extend(str(item) for item in child.get("warnings") or [])
            errors.extend(str(item) for item in child.get("errors") or [])

        result = merge_child_results(children, started_at=started_at)
        if warnings:
            result["warnings"] = warnings[:50]
        if errors:
            result["errors"] = errors[:50]

        for child in children:
            if child.get("status") == "error" and not child.get("continue_on_error", True):
                result["continued_on_failure"] = False
                raise RuntimeError(
                    f"master governance failed before {child.get('job_name')}: "
                    f"{child.get('errors', [])}"
                )
        if any(child.get("status") == "error" for child in children):
            result["continued_on_failure"] = True
        return result


class AShareStockPolicy:
    scope = "a_share_stock"

    def __init__(self, manager: Any, config: Dict[str, Any]):
        self.manager = manager
        self.config = config

    async def execute(self, requirement: MasterGovernanceRequirement) -> Dict[str, Any]:
        exchanges = requirement.exchanges or ["SSE", "SZSE", "BSE"]
        started_at = get_shanghai_time()
        freshness_hours = (
            requirement.freshness_threshold_hours
            if requirement.freshness_threshold_hours is not None
            else self.config.get("freshness_threshold_hours")
        )
        if requirement.mode == "freshness_gated" and self.config.get("reuse_fresh_master", True):
            try:
                fresh = await self.manager._build_fresh_master_governance_result(
                    exchanges=exchanges,
                    freshness_threshold_hours=freshness_hours,
                    job_name=requirement.job_name,
                    job_type=requirement.job_type,
                    started_at=started_at,
                    unsupported_exchanges=[],
                )
            except Exception as exc:
                logger.warning(
                    "A-share stock master freshness check failed before %s: %s; running sync",
                    requirement.job_name,
                    exc,
                )
                fresh = None
            if fresh is not None:
                return fresh

        result = await self.manager.sync_instrument_master(
            exchanges,
            include_pytdx_validation=(
                requirement.include_pytdx_validation
                if requirement.include_pytdx_validation is not None
                else self.config.get("pytdx_validation_enabled", False)
            ),
            timeout_sec=(
                requirement.timeout_sec
                if requirement.timeout_sec is not None
                else self.config.get("timeout_sec")
            ),
            freshness_threshold_hours=freshness_hours,
        )
        result["action"] = "synced"
        result.setdefault("reason", "master_sync_executed")
        return result


class AShareIndexPolicy:
    scope = "a_share_index"

    def __init__(self, manager: Any, config: Dict[str, Any]):
        self.manager = manager
        self.config = config

    async def execute(self, requirement: MasterGovernanceRequirement) -> Dict[str, Any]:
        exchanges = requirement.exchanges or ["SSE", "SZSE"]
        result = await self.manager.sync_index_master(
            exchanges=exchanges,
            target_date=requirement.target_date,
            timeout_sec=(
                requirement.timeout_sec
                if requirement.timeout_sec is not None
                else self.config.get("timeout_sec")
            ),
        )
        result.setdefault("action", "index_master_governance")
        result.setdefault("reason", "index_master_governance_executed")
        return result


class HKEXInstrumentPolicy:
    scope = "hkex_instrument"

    def __init__(self, manager: Any, config: Dict[str, Any]):
        self.manager = manager
        self.config = config

    async def execute(self, requirement: MasterGovernanceRequirement) -> Dict[str, Any]:
        mode = str(requirement.mode or "").strip().lower()
        if mode not in SUPPORTED_MODES_BY_SCOPE["hkex_instrument"]:
            return _child_result(
                requirement,
                status="error",
                action="unsupported",
                reason="unsupported_hkex_governance_mode",
                errors=[f"unsupported HKEX governance mode: {mode}"],
            )
        result = await self.manager.sync_hkex_instrument_master(
            mode=mode,
            timeout_sec=(
                requirement.timeout_sec
                if requirement.timeout_sec is not None
                else self.config.get("timeout_sec")
            ),
        )
        result.setdefault("action", "synced")
        result.setdefault("reason", "hkex_instrument_master_governance_executed")
        return result


def validate_requirements(
    requirements: Sequence[MasterGovernanceRequirement],
) -> List[MasterGovernanceRequirement]:
    seen = set()
    normalized: List[MasterGovernanceRequirement] = []
    for requirement in requirements:
        requirement.validate()
        key = requirement.key()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(requirement)
    return normalized


def merge_child_results(
    children: Sequence[Dict[str, Any]],
    *,
    started_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    started_at = started_at or get_shanghai_time()
    status = _aggregate_status([child.get("status") for child in children])
    action = "skipped" if status == "skipped" else "governed"
    summary = _merge_summaries(children)
    exchanges: Dict[str, Any] = {}
    source_priority: List[str] = []
    for child in children:
        for exchange, exchange_result in (child.get("exchanges") or {}).items():
            exchanges[exchange] = exchange_result
        for item in child.get("source_priority") or []:
            if item not in source_priority:
                source_priority.append(item)
    finished_at = get_shanghai_time()
    result = {
        "status": status,
        "action": action,
        "reason": _aggregate_reason(children, status),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "elapsed_sec": round((finished_at - started_at).total_seconds(), 3),
        "summary": summary,
        "exchanges": exchanges,
        "source_priority": source_priority,
        "children": list(children),
        "warnings": _bounded_messages(children, "warnings"),
        "errors": _bounded_messages(children, "errors"),
    }
    if len(children) == 1:
        child = children[0]
        for key in ("index_master_governance", "mode"):
            if key in child:
                result[key] = child[key]
        if child.get("scope") in {"a_share_stock", "hkex_instrument"}:
            result["stock_master_governance"] = child
        result["action"] = child.get("action") or result["action"]
        result["reason"] = child.get("reason") or result["reason"]
    else:
        for child in children:
            if child.get("scope") == "a_share_index":
                result["index_master_governance"] = child
            elif child.get("scope") in {"a_share_stock", "hkex_instrument"}:
                result["stock_master_governance"] = child
    return result


def _normalize_strings(values: Iterable[Any]) -> List[str]:
    return [str(value).strip().upper() for value in values if str(value).strip()]


def _optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    return int(value)


def _optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    return float(value)


def _optional_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    return bool(value)


def _base_result(
    *,
    status: str,
    action: str,
    reason: str,
    started_at: datetime,
    children: Sequence[Dict[str, Any]],
    warnings: List[str],
    errors: List[str],
) -> Dict[str, Any]:
    finished_at = get_shanghai_time()
    return {
        "status": status,
        "action": action,
        "reason": reason,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "elapsed_sec": round((finished_at - started_at).total_seconds(), 3),
        "summary": _merge_summaries(children),
        "exchanges": {},
        "children": list(children),
        "warnings": warnings,
        "errors": errors,
    }


def _child_result(
    requirement: MasterGovernanceRequirement,
    *,
    status: str,
    action: str,
    reason: str,
    warnings: Optional[List[str]] = None,
    errors: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return {
        "status": status,
        "action": action,
        "reason": reason,
        "summary": {
            "exchanges": requirement.exchanges,
            "added_instruments": 0,
            "deactivated_instruments": 0,
            "active_count": 0,
        },
        "exchanges": {},
        "warnings": list(warnings or []),
        "errors": list(errors or []),
    }


def _normalize_child_result(
    requirement: MasterGovernanceRequirement,
    child: Dict[str, Any],
) -> Dict[str, Any]:
    normalized = dict(child or {})
    normalized.setdefault("status", "unknown")
    normalized.setdefault("action", normalized.get("reason") or "unknown")
    normalized.setdefault("reason", normalized.get("action") or "unknown")
    normalized.setdefault("summary", {})
    normalized.setdefault("exchanges", {})
    normalized.setdefault("warnings", [])
    normalized.setdefault("errors", [])
    normalized["scope"] = requirement.scope
    normalized["mode"] = requirement.mode if requirement.scope != "hkex_instrument" else normalized.get("mode", requirement.mode)
    normalized["job_name"] = requirement.job_name
    normalized["job_type"] = requirement.job_type
    normalized["target_date"] = requirement.target_date.isoformat() if requirement.target_date else None
    normalized["continue_on_error"] = requirement.continue_on_error
    normalized["legacy_fallback"] = requirement.legacy_fallback
    normalized["requirement"] = requirement.as_dict()
    return normalized


def _aggregate_status(statuses: Sequence[Any]) -> str:
    normalized = [str(status or "unknown").lower() for status in statuses]
    if not normalized:
        return "skipped"
    if any(status in {"error", "failed"} for status in normalized):
        return "error"
    if any(status in {"warning", "degraded"} for status in normalized):
        return "warning"
    if all(status in {"skipped", "disabled", "unavailable"} for status in normalized):
        return "skipped"
    if all(status == "fresh" for status in normalized):
        return "fresh"
    if any(status == "fresh" for status in normalized) and all(
        status in {"fresh", "success"} for status in normalized
    ):
        return "success"
    if all(status == "success" for status in normalized):
        return "success"
    return "warning"


def _aggregate_reason(children: Sequence[Dict[str, Any]], status: str) -> str:
    if not children:
        return "no_master_governance_requirements"
    if len(children) == 1:
        return str(children[0].get("reason") or children[0].get("action") or status)
    reasons = {
        str(child.get("reason") or child.get("action") or "")
        for child in children
        if child.get("reason") or child.get("action")
    }
    if len(reasons) == 1:
        return next(iter(reasons))
    return f"merged_{len(children)}_master_governance_policy_results"


def _merge_summaries(children: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "exchanges": [],
        "added_instruments": 0,
        "deactivated_instruments": 0,
        "suspended_instruments": 0,
        "reactivated_instruments": 0,
        "review_required": 0,
        "active_count": 0,
    }
    samples: List[Dict[str, Any]] = []
    source_usage: Dict[str, int] = {}
    source_authority: Dict[str, int] = {}
    for child in children:
        child_summary = child.get("summary") or {}
        for exchange in child_summary.get("exchanges") or []:
            if exchange not in summary["exchanges"]:
                summary["exchanges"].append(exchange)
        for key in (
            "added_instruments",
            "deactivated_instruments",
            "suspended_instruments",
            "reactivated_instruments",
            "review_required",
            "active_count",
            "master_rows_saved",
            "evidence_rows_saved",
            "lifecycle_skip_count",
            "direct_terminated_count",
            "inferred_terminated_count",
            "stale_no_quote_count",
            "ambiguous_master_duplicate_groups_skipped",
            "collapsed_duplicate_master_rows",
        ):
            if key in child_summary:
                summary[key] = int(summary.get(key, 0) or 0) + int(child_summary.get(key, 0) or 0)
        for sample in child_summary.get("samples") or []:
            if len(samples) < 10:
                samples.append(sample)
        if isinstance(child_summary.get("source_usage"), dict):
            for source, count in child_summary["source_usage"].items():
                try:
                    source_usage[source] = source_usage.get(source, 0) + int(count or 0)
                except (TypeError, ValueError):
                    source_usage[source] = source_usage.get(source, 0)
        if isinstance(child_summary.get("source_authority"), dict):
            for authority, count in child_summary["source_authority"].items():
                try:
                    source_authority[authority] = (
                        source_authority.get(authority, 0) + int(count or 0)
                    )
                except (TypeError, ValueError):
                    source_authority[authority] = source_authority.get(authority, 0)
    if samples:
        summary["samples"] = samples
    if source_usage:
        summary["source_usage"] = source_usage
    if source_authority:
        summary["source_authority"] = source_authority
    return summary


def _bounded_messages(children: Sequence[Dict[str, Any]], key: str, limit: int = 20) -> List[str]:
    messages: List[str] = []
    for child in children:
        prefix = child.get("scope")
        for item in child.get(key) or []:
            text = str(item)
            if prefix and not text.startswith(f"{prefix}:"):
                text = f"{prefix}: {text}"
            messages.append(text)
            if len(messages) >= limit:
                return messages
    return messages


async def maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value
