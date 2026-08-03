"""Bounded, resumable orchestration for business-profile semantic production."""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, MutableMapping, Sequence


PIPELINE_SCHEMA_VERSION = "business_profile_semantic_pipeline.v1"
CHECKPOINT_SCHEMA_VERSION = "business_profile_semantic_checkpoint.v1"
PIPELINE_STAGES = ("plan", "select", "extract", "verify", "promote")
PIPELINE_MODES = (*PIPELINE_STAGES, "resume", "report", "rebuild-publications")


@dataclass(frozen=True)
class SemanticProductionBudgets:
    max_documents: int = 3
    max_pages: int = 40
    max_characters: int = 120_000
    max_tokens: int = 20_000
    max_cost: float = 5.0
    max_elapsed_seconds: float = 900.0
    max_errors: int = 10
    max_concurrency: int = 2


@dataclass(frozen=True)
class SemanticProductionThresholds:
    max_unsupported_output_rate: float = 0.01
    max_conflict_rate: float = 0.02
    max_drift_rate: float = 0.03
    max_exception_backlog: int = 100


@dataclass(frozen=True)
class SemanticProductionConfig:
    enabled: bool = False
    promotion_enabled: bool = False
    scheduler_enabled: bool = False
    retry_limit: int = 3
    budgets: SemanticProductionBudgets = field(default_factory=SemanticProductionBudgets)
    thresholds: SemanticProductionThresholds = field(
        default_factory=SemanticProductionThresholds
    )
    kill_switches: Mapping[str, bool] = field(
        default_factory=lambda: {
            "all_writes": False,
            "network_calls": False,
            "promotion": False,
            "scope_widening": False,
        }
    )

    def validate(self) -> None:
        if self.retry_limit < 0:
            raise ValueError("semantic production retry_limit cannot be negative")
        for key, value in asdict(self.budgets).items():
            if float(value) <= 0:
                raise ValueError(f"semantic production budget must be positive: {key}")
        for key, value in asdict(self.thresholds).items():
            if key == "max_exception_backlog":
                if int(value) < 0:
                    raise ValueError("max_exception_backlog cannot be negative")
            elif not 0 <= float(value) <= 1:
                raise ValueError(f"semantic production threshold out of range: {key}")
        required_switches = {
            "all_writes",
            "network_calls",
            "promotion",
            "scope_widening",
        }
        if set(self.kill_switches) != required_switches or any(
            not isinstance(value, bool) for value in self.kill_switches.values()
        ):
            raise ValueError("semantic production kill switches are incomplete")
        if self.scheduler_enabled and not self.enabled:
            raise ValueError("semantic scheduler requires semantic production enablement")
        if self.promotion_enabled and not self.enabled:
            raise ValueError("semantic promotion requires semantic production enablement")


@dataclass(frozen=True)
class SemanticProductionScope:
    instruments: tuple[str, ...]
    field_families: tuple[str, ...]
    knowledge_cutoff: str
    identities: Mapping[str, str]
    promotion_manifest_hashes: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.instruments or not all(str(item).strip() for item in self.instruments):
            raise ValueError("semantic production scope requires instruments")
        if not self.field_families or not all(
            str(item).strip() for item in self.field_families
        ):
            raise ValueError("semantic production scope requires field families")
        if not self.knowledge_cutoff:
            raise ValueError("semantic production scope requires knowledge cutoff")
        if not self.identities or any(
            not str(key).strip() or not str(value).strip()
            for key, value in self.identities.items()
        ):
            raise ValueError("semantic production scope identities are incomplete")

    @property
    def scope_hash(self) -> str:
        return _stable_hash(
            {
                "instruments": sorted(set(self.instruments)),
                "field_families": sorted(set(self.field_families)),
                "knowledge_cutoff": self.knowledge_cutoff,
                "identities": dict(sorted(self.identities.items())),
                "promotion_manifest_hashes": dict(
                    sorted(self.promotion_manifest_hashes.items())
                ),
            }
        )


class SemanticProductionCheckpointStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def load(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
            raise ValueError("unsupported semantic production checkpoint schema")
        return payload

    def save(self, payload: Mapping[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(
            dict(payload),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        temp_path = self.path.with_name(f".{self.path.name}.{os.getpid()}.tmp")
        temp_path.write_text(encoded, encoding="utf-8")
        temp_path.replace(self.path)


class BusinessProfileSemanticPipeline:
    """Execute bounded stage handlers with exact-scope checkpoint semantics."""

    def __init__(
        self,
        *,
        config: SemanticProductionConfig,
        checkpoint_store: SemanticProductionCheckpointStore,
        handlers: Mapping[str, Callable[..., Mapping[str, Any]]] | None = None,
        cancellation_requested: Callable[[], bool] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        config.validate()
        self.config = config
        self.checkpoint_store = checkpoint_store
        self.handlers = dict(handlers or {})
        self.cancellation_requested = cancellation_requested or (lambda: False)
        self.clock = clock

    def run(
        self,
        mode: str,
        *,
        scope: SemanticProductionScope,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_mode = str(mode or "").strip()
        if normalized_mode not in PIPELINE_MODES:
            raise ValueError(f"unsupported semantic production mode: {mode}")
        if not self.config.enabled and normalized_mode != "report":
            return {"status": "disabled", "reason": "semantic_production_disabled"}
        checkpoint = self._load_or_create(scope)
        if normalized_mode == "report":
            return self._report(checkpoint)
        if self.config.kill_switches["all_writes"]:
            return self._stop(checkpoint, "kill_switch:all_writes")
        stage = self._resolve_stage(normalized_mode, checkpoint)
        if stage is None:
            return {"status": "unchanged", **self._report(checkpoint)}
        if stage == "promote" and (
            not self.config.promotion_enabled
            or self.config.kill_switches["promotion"]
            or not scope.promotion_manifest_hashes
        ):
            return self._stop(checkpoint, "promotion_disabled_or_unmanifested")
        if self.cancellation_requested():
            return self._stop(checkpoint, "cancelled")
        handler = self.handlers.get(stage)
        if handler is None:
            raise ValueError(f"semantic production stage handler is not configured: {stage}")
        start = self.clock()
        result = dict(
            handler(
                scope=scope,
                payload=dict(payload or {}),
                checkpoint=dict(checkpoint),
                config=self.config,
            )
        )
        elapsed = max(0.0, self.clock() - start)
        metrics = _merge_metrics(
            checkpoint.get("metrics") or {},
            result.get("metrics") or {},
            elapsed_seconds=elapsed,
        )
        checkpoint["metrics"] = metrics
        stop_reason = self._budget_stop_reason(metrics)
        if stop_reason:
            checkpoint["artifacts"][stage] = result.get("artifact")
            return self._stop(checkpoint, stop_reason)
        result_status = str(result.get("status") or "").strip().lower()
        if result_status in {"interrupted", "cancelled", "stopped"}:
            checkpoint["artifacts"][stage] = result.get("artifact")
            return self._stop(checkpoint, str(result.get("reason") or result["status"]))
        if result_status not in {"success", "completed", "unchanged"}:
            checkpoint["artifacts"][stage] = result.get("artifact")
            reason = str(result.get("reason") or result_status or "missing_status")
            return self._stop(checkpoint, f"stage_failed:{stage}:{reason}")
        checkpoint["completed_stages"] = list(
            dict.fromkeys([*checkpoint["completed_stages"], stage])
        )
        checkpoint["artifacts"][stage] = result.get("artifact")
        checkpoint["status"] = "completed" if stage == PIPELINE_STAGES[-1] else "partial"
        checkpoint["stopped_reason"] = None
        self.checkpoint_store.save(checkpoint)
        return {
            "status": "success",
            "stage": stage,
            "artifact": result.get("artifact"),
            "checkpoint_hash": _stable_hash(checkpoint),
            **self._report(checkpoint),
        }

    def _load_or_create(self, scope: SemanticProductionScope) -> dict[str, Any]:
        existing = self.checkpoint_store.load()
        budgets_hash = _stable_hash(asdict(self.config.budgets))
        if existing is not None:
            if existing.get("scope_hash") != scope.scope_hash:
                raise ValueError("stale semantic production checkpoint scope")
            if existing.get("budgets_hash") != budgets_hash:
                raise ValueError("stale semantic production checkpoint budgets")
            return existing
        checkpoint = {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "pipeline_schema_version": PIPELINE_SCHEMA_VERSION,
            "scope_hash": scope.scope_hash,
            "scope": {
                "instruments": list(scope.instruments),
                "field_families": list(scope.field_families),
                "knowledge_cutoff": scope.knowledge_cutoff,
                "identities": dict(scope.identities),
                "promotion_manifest_hashes": dict(scope.promotion_manifest_hashes),
            },
            "budgets_hash": budgets_hash,
            "completed_stages": [],
            "artifacts": {},
            "metrics": {},
            "status": "pending",
            "stopped_reason": None,
        }
        self.checkpoint_store.save(checkpoint)
        return checkpoint

    @staticmethod
    def _resolve_stage(mode: str, checkpoint: Mapping[str, Any]) -> str | None:
        completed = set(checkpoint.get("completed_stages") or [])
        if mode == "resume":
            return next((stage for stage in PIPELINE_STAGES if stage not in completed), None)
        if mode == "rebuild-publications":
            return "promote"
        if mode in completed:
            return None
        expected = next((stage for stage in PIPELINE_STAGES if stage not in completed), None)
        if mode != expected:
            raise ValueError(
                f"semantic production stage order violation: expected={expected} actual={mode}"
            )
        return mode

    def _budget_stop_reason(self, metrics: Mapping[str, Any]) -> str | None:
        budgets = self.config.budgets
        comparisons = (
            ("documents", budgets.max_documents),
            ("pages", budgets.max_pages),
            ("characters", budgets.max_characters),
            ("tokens", budgets.max_tokens),
            ("cost", budgets.max_cost),
            ("elapsed_seconds", budgets.max_elapsed_seconds),
            ("errors", budgets.max_errors),
        )
        for key, maximum in comparisons:
            if float(metrics.get(key) or 0) > float(maximum):
                return f"budget_exhausted:{key}"
        thresholds = self.config.thresholds
        for key, maximum in (
            ("unsupported_output_rate", thresholds.max_unsupported_output_rate),
            ("conflict_rate", thresholds.max_conflict_rate),
            ("drift_rate", thresholds.max_drift_rate),
        ):
            if float(metrics.get(key) or 0) > float(maximum):
                return f"quality_stop:{key}"
        if int(metrics.get("exception_backlog") or 0) > thresholds.max_exception_backlog:
            return "quality_stop:exception_backlog"
        return None

    def _stop(self, checkpoint: MutableMapping[str, Any], reason: str) -> dict[str, Any]:
        checkpoint["status"] = "stopped"
        checkpoint["stopped_reason"] = reason
        self.checkpoint_store.save(checkpoint)
        return {"status": "stopped", "reason": reason, **self._report(checkpoint)}

    @staticmethod
    def _report(checkpoint: Mapping[str, Any]) -> dict[str, Any]:
        metrics = dict(checkpoint.get("metrics") or {})
        return {
            "pipeline_status": checkpoint.get("status"),
            "scope_hash": checkpoint.get("scope_hash"),
            "completed_stages": list(checkpoint.get("completed_stages") or []),
            "stopped_reason": checkpoint.get("stopped_reason"),
            "metrics": {
                "reused_results": int(metrics.get("reused_results") or 0),
                "documents": int(metrics.get("documents") or 0),
                "pages": int(metrics.get("pages") or 0),
                "deterministic_completed": int(
                    metrics.get("deterministic_completed") or 0
                ),
                "llm_calls": int(metrics.get("llm_calls") or 0),
                "tokens": int(metrics.get("tokens") or 0),
                "cost": float(metrics.get("cost") or 0),
                "elapsed_seconds": float(metrics.get("elapsed_seconds") or 0),
                "auto_promoted": int(metrics.get("auto_promoted") or 0),
                "machine_rework_recovered": int(
                    metrics.get("machine_rework_recovered") or 0
                ),
                "quick_review": int(metrics.get("quick_review") or 0),
                "deep_review": int(metrics.get("deep_review") or 0),
                "conflicts": int(metrics.get("conflicts") or 0),
                "unsupported_output_rate": float(
                    metrics.get("unsupported_output_rate") or 0
                ),
                "drift_rate": float(metrics.get("drift_rate") or 0),
                "candidate_valuation_leakage": int(
                    metrics.get("candidate_valuation_leakage") or 0
                ),
            },
        }


def parse_semantic_production_config(value: Mapping[str, Any]) -> SemanticProductionConfig:
    payload = dict(value or {})
    config = SemanticProductionConfig(
        enabled=payload.get("enabled") is True,
        promotion_enabled=payload.get("promotion_enabled") is True,
        scheduler_enabled=payload.get("scheduler_enabled") is True,
        retry_limit=int(payload.get("retry_limit", 3)),
        budgets=SemanticProductionBudgets(**dict(payload.get("budgets") or {})),
        thresholds=SemanticProductionThresholds(
            **dict(payload.get("thresholds") or {})
        ),
        kill_switches=dict(
            payload.get("kill_switches")
            or SemanticProductionConfig().kill_switches
        ),
    )
    config.validate()
    return config


def _merge_metrics(
    current: Mapping[str, Any],
    update: Mapping[str, Any],
    *,
    elapsed_seconds: float,
) -> dict[str, Any]:
    output = dict(current)
    gauges = {
        "unsupported_output_rate",
        "conflict_rate",
        "drift_rate",
        "exception_backlog",
    }
    for key, value in update.items():
        if key in gauges:
            output[key] = value
        elif isinstance(value, (int, float)):
            output[key] = float(output.get(key) or 0) + float(value)
    output["elapsed_seconds"] = float(output.get("elapsed_seconds") or 0) + elapsed_seconds
    return output


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()
