"""Shared financial statement maintenance repair routing.

This module keeps source-selection and fallback rules out of scheduler-facing
tasks. Callers provide canonical repair targets; the router decides which
structured source to attempt first and how to re-check canonical readiness.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from research.financial_disclosure_events import (
    ACCEPTED_FINANCIAL_DISCLOSURE_CLASSIFICATIONS,
)
from research.storage import ResearchStorageManager
from utils.config_manager import ResearchConfig


@dataclass(frozen=True)
class FinancialMaintenanceRepairTarget:
    """Canonical instrument-period repair target shared by maintenance jobs."""

    instrument_id: str
    symbol: str
    exchange: str
    report_period: str
    profile: str
    classification: str = "local_core_gap"

    @property
    def key(self) -> Tuple[str, str]:
        return (self.instrument_id, self.report_period)


class FinancialMaintenanceRepairRouter:
    """Route financial maintenance repair through official and fallback sources."""

    def __init__(
        self,
        *,
        storage: ResearchStorageManager,
        research_config: ResearchConfig,
    ) -> None:
        self.storage = storage
        self.research_config = research_config

    def source_order(self) -> List[str]:
        module_cfg = self.research_config.modules.get("financial_statements", {})
        maintenance_cfg = module_cfg.get("disclosure_incremental_sync", {})
        raw_order = maintenance_cfg.get("repair_source_order")
        if raw_order is None:
            raw_order = (
                module_cfg.get("maintenance_repair_source_order")
                or ["cninfo_data20", "ths_report", "sina_report"]
            )
        return [str(item).strip() for item in raw_order if str(item).strip()]

    def fallback_statement_sources(
        self,
        source_order: Optional[Sequence[str]] = None,
    ) -> List[str]:
        return [
            source
            for source in (source_order or self.source_order())
            if source != "cninfo_data20"
        ]

    def default_summary(self) -> Dict[str, Any]:
        return {
            "source_order": self.source_order(),
            "cninfo_attempts": 0,
            "cninfo_successes": 0,
            "cninfo_batch_successes": 0,
            "cninfo_failed_instrument_periods": 0,
            "cninfo_missing_or_ambiguous": 0,
            "fallback_attempts": 0,
            "fallback_successes": 0,
            "fallback_sources": self.fallback_statement_sources(),
            "errors": [],
        }

    async def repair_targets(
        self,
        *,
        targets: Sequence[FinancialMaintenanceRepairTarget],
        required_core_facts: Sequence[str],
        mapping_version: str,
        db_path: Path,
        request_interval_seconds: float,
        request_timeout_seconds: float,
        accepted_gap_classifications: Sequence[
            str
        ] = ACCEPTED_FINANCIAL_DISCLOSURE_CLASSIFICATIONS,
    ) -> Dict[str, Any]:
        from scripts.dev_validation.live_audit_sina_ths_local_core import LiveAuditTarget
        from scripts.dev_validation.validate_sina_ths_local_core_dryrun import (
            run_local_core_dryrun,
        )

        source_order = self.source_order()
        fallback_sources = self.fallback_statement_sources(source_order)
        summary = self.default_summary()
        summary["source_order"] = source_order
        summary["fallback_sources"] = fallback_sources

        if "cninfo_data20" in source_order:
            cninfo_summary = await self._run_cninfo_data20_import(
                targets=targets,
                db_path=db_path,
                request_interval_seconds=request_interval_seconds,
                request_timeout_seconds=request_timeout_seconds,
            )
            cninfo_ready_keys = self._ready_target_keys(
                targets,
                required_core_facts=required_core_facts,
                mapping_version=mapping_version,
            )
            summary.update(
                {
                    "cninfo_attempts": cninfo_summary.get("attempts", 0),
                    "cninfo_successes": len(cninfo_ready_keys),
                    "cninfo_batch_successes": cninfo_summary.get(
                        "batch_successes",
                        0,
                    ),
                    "cninfo_failed_instrument_periods": cninfo_summary.get(
                        "failed_instrument_periods",
                        0,
                    ),
                    "cninfo_missing_or_ambiguous": max(
                        0,
                        int(cninfo_summary.get("attempts", 0)) - len(cninfo_ready_keys),
                    ),
                }
            )
            summary["errors"].extend(cninfo_summary.get("errors") or [])

        ready_keys_after_cninfo = self._ready_target_keys(
            targets,
            required_core_facts=required_core_facts,
            mapping_version=mapping_version,
        )
        fallback_targets = [
            target
            for target in targets
            if target.key not in ready_keys_after_cninfo
        ]
        summary["fallback_attempts"] = len(fallback_targets)
        if not fallback_targets or not fallback_sources:
            return summary

        grouped: Dict[str, List[FinancialMaintenanceRepairTarget]] = {}
        for target in fallback_targets:
            grouped.setdefault(target.report_period, []).append(target)
        accepted_classification_set = set(accepted_gap_classifications)
        accepted_source_gaps = {
            target.key: {
                "facts": set(required_core_facts),
                "classification": target.classification,
            }
            for target in fallback_targets
            if target.classification in accepted_classification_set
        }
        for report_period, period_targets in grouped.items():
            live_targets = [
                LiveAuditTarget(
                    target.instrument_id,
                    target.exchange,
                    target.profile,
                )
                for target in period_targets
            ]
            await run_local_core_dryrun(
                targets=live_targets,
                report_periods=[report_period],
                db_path=db_path,
                mapping_version=mapping_version,
                source_order=fallback_sources,
                required_canonical_facts=required_core_facts,
                request_interval_seconds=request_interval_seconds,
                request_timeout_seconds=request_timeout_seconds,
                accepted_source_gaps=accepted_source_gaps,
                accepted_source_gap_exchanges=(),
            )
        ready_keys_after_fallback = self._ready_target_keys(
            fallback_targets,
            required_core_facts=required_core_facts,
            mapping_version=mapping_version,
        )
        summary["fallback_successes"] = len(ready_keys_after_fallback)
        return summary

    def _ready_target_keys(
        self,
        targets: Sequence[FinancialMaintenanceRepairTarget],
        *,
        required_core_facts: Sequence[str],
        mapping_version: str,
    ) -> set[Tuple[str, str]]:
        """Return targets that are ready, using a fresh storage view after writes."""
        ready = self._ready_target_keys_with_storage(
            self.storage,
            targets,
            required_core_facts=required_core_facts,
            mapping_version=mapping_version,
        )
        if len(ready) == len(targets):
            return ready
        try:
            fresh_storage = ResearchStorageManager(self.research_config)
            ready |= self._ready_target_keys_with_storage(
                fresh_storage,
                targets,
                required_core_facts=required_core_facts,
                mapping_version=mapping_version,
            )
        except Exception:
            return ready
        return ready

    def _ready_target_keys_with_storage(
        self,
        storage: ResearchStorageManager,
        targets: Sequence[FinancialMaintenanceRepairTarget],
        *,
        required_core_facts: Sequence[str],
        mapping_version: str,
    ) -> set[Tuple[str, str]]:
        router = (
            self
            if storage is self.storage
            else FinancialMaintenanceRepairRouter(
                storage=storage,
                research_config=self.research_config,
            )
        )
        ready: set[Tuple[str, str]] = set()
        for target in targets:
            if router.readiness_for_target(
                target,
                required_core_facts=required_core_facts,
                mapping_version=mapping_version,
            ).get("ready"):
                ready.add(target.key)
        return ready

    async def _run_cninfo_data20_import(
        self,
        *,
        targets: Sequence[FinancialMaintenanceRepairTarget],
        db_path: Path,
        request_interval_seconds: float,
        request_timeout_seconds: float,
    ) -> Dict[str, Any]:
        if not targets:
            return {
                "attempts": 0,
                "successes": 0,
                "missing_or_ambiguous": 0,
                "errors": [],
            }
        from scripts.dev_validation.validate_sse_official_financial_json_batches_live import (
            run_batches,
        )

        attempts = len(targets)
        batch_successes = 0
        failed_instrument_periods = 0
        errors: List[str] = []
        grouped: Dict[Tuple[str, str], List[FinancialMaintenanceRepairTarget]] = {}
        for target in targets:
            grouped.setdefault((target.exchange, target.report_period), []).append(target)
        with tempfile.TemporaryDirectory(
            prefix="quote_financial_cninfo_maintenance_"
        ) as temp_name:
            temp_dir = Path(temp_name)
            for (exchange, report_period), period_targets in grouped.items():
                try:
                    result = await run_batches(
                        instrument_ids=[
                            target.instrument_id for target in period_targets
                        ],
                        exchange=exchange,
                        official_source="cninfo",
                        report_periods=[report_period],
                        db_path=db_path,
                        batch_size=max(1, len(period_targets)),
                        batch_timeout_seconds=max(
                            60.0,
                            request_timeout_seconds * len(period_targets) * 3,
                        ),
                        request_timeout_seconds=request_timeout_seconds,
                        request_interval_seconds=request_interval_seconds,
                        checkpoint_path=temp_dir
                        / f"{exchange}_{report_period}.checkpoint.json",
                        include_batch_details=False,
                    )
                    failed_count = int(result.get("failed_instrument_period_count") or 0)
                    failed_instrument_periods += failed_count
                    batch_successes += max(
                        0,
                        len(period_targets) - failed_count,
                    )
                    if result.get("status") not in {"passed", "success"}:
                        errors.append(
                            "cninfo_data20:"
                            f"{exchange}:{report_period}:{result.get('status')}"
                            f":failed={failed_count}/{len(period_targets)}"
                        )
                except Exception as exc:
                    failed_instrument_periods += len(period_targets)
                    errors.append(f"cninfo_data20:{exchange}:{report_period}:{exc}")
        return {
            "attempts": attempts,
            "batch_successes": batch_successes,
            "failed_instrument_periods": failed_instrument_periods,
            "missing_or_ambiguous": max(0, attempts - batch_successes),
            "errors": errors[:10],
        }

    def readiness_for_target(
        self,
        target: FinancialMaintenanceRepairTarget,
        *,
        required_core_facts: Sequence[str],
        mapping_version: str,
    ) -> Dict[str, Any]:
        with self.storage.financial_database_scope():
            result = self.storage.financial_statements.get_local_core_facts(
                target.instrument_id,
                report_period=target.report_period,
                requested_canonical_facts=list(required_core_facts),
                profile=target.profile,
                mapping_version=mapping_version,
                include_history=True,
            )
            return self._merge_official_cninfo_readiness(
                result,
                target=target,
                required_core_facts=required_core_facts,
            )

    def _merge_official_cninfo_readiness(
        self,
        result: Dict[str, Any],
        *,
        target: FinancialMaintenanceRepairTarget,
        required_core_facts: Sequence[str],
    ) -> Dict[str, Any]:
        missing = result.get("missing_fields") or []
        missing_facts = {
            str(item.get("canonical_fact"))
            for item in missing
            if item.get("canonical_fact")
        }
        if not missing_facts:
            return result
        official_facts = self._official_cninfo_facts_by_canonical(target)
        if not official_facts:
            return result
        merged = dict(result)
        facts = dict(merged.get("facts") or {})
        for fact_name in sorted(missing_facts):
            if fact_name in official_facts:
                fact = dict(official_facts[fact_name])
                raw_fact = dict(fact.get("raw_fact") or {})
                raw_fact["maintenance_source_routing"] = {
                    "source": "cninfo_data20",
                    "role": "official_structured_first",
                }
                fact["raw_fact"] = raw_fact
                facts[fact_name] = fact
        merged["facts"] = facts
        merged["missing_fields"] = [
            item
            for item in missing
            if str(item.get("canonical_fact") or "") not in facts
        ]
        merged["ready"] = not merged["missing_fields"]
        return merged

    def _official_cninfo_facts_by_canonical(
        self,
        target: FinancialMaintenanceRepairTarget,
    ) -> Dict[str, Dict[str, Any]]:
        getter = getattr(self.storage.financial_statements, "get_numeric_facts", None)
        if getter is None:
            return {}
        try:
            rows = getter(
                target.instrument_id,
                report_period=target.report_period,
                include_history=True,
            )
        except TypeError:
            return {}
        official: Dict[str, Dict[str, Any]] = {}
        for row in rows or []:
            canonical = str(row.get("canonical_fact_name") or "")
            if not canonical:
                continue
            if not self._is_cninfo_data20_numeric_fact(row):
                continue
            official.setdefault(canonical, row)
        return official

    @staticmethod
    def _is_cninfo_data20_numeric_fact(row: Mapping[str, Any]) -> bool:
        raw_fact = row.get("raw_fact") or {}
        haystack = " ".join(
            str(value or "")
            for value in (
                row.get("source"),
                row.get("source_mode"),
                row.get("parser_version"),
                raw_fact.get("source_profile"),
                raw_fact.get("parser_profile"),
                raw_fact.get("source"),
            )
        ).lower()
        return "cninfo" in haystack or "data20" in haystack
