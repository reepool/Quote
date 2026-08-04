"""Versioned resource catalog and admission gates for backtest-critical data."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional


CATALOG_SCHEMA_VERSION = "backtest-data-resources.v1"
ROUTE_DECISIONS = {
    "reuse",
    "extend_existing",
    "new_source_required",
    "manual_import_only",
    "unavailable",
}
TEMPORAL_CONTRACTS = {
    "knowledge_time_safe",
    "effective_date_only",
    "current_only",
    "unavailable",
}
KNOWN_PARENT_JOBS = {
    "daily_data_update",
    "index_master_governance_sync",
    "a_share_daily_data_historical_backfill",
    "a_share_stock_master_sync",
    "financial_disclosure_incremental_sync",
    "financial_disclosure_reconciliation_sync",
    "a_share_cninfo_corporate_action_daily_sync",
    "a_share_tdx_corporate_action_weekly_full_refresh",
    "industry_standard_sync",
    "industry_index_analysis_sync",
}
KNOWN_STORES = {
    "quotes.db",
    "financials.db",
    "research.db",
}


class CatalogValidationError(ValueError):
    """Raised when a resource route violates integration admission rules."""


def _strings(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        values: Iterable[Any] = (value,)
    elif isinstance(value, Iterable):
        values = value
    else:
        values = (value,)
    return tuple(dict.fromkeys(str(item).strip() for item in values if str(item).strip()))


@dataclass(frozen=True)
class ProviderRoute:
    name: str
    existing: bool = True
    full_market: bool = False
    capabilities: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ProviderRoute":
        name = str(value.get("name") or "").strip()
        if not name:
            raise CatalogValidationError("provider name is required")
        return cls(
            name=name,
            existing=bool(value.get("existing", True)),
            full_market=bool(value.get("full_market", False)),
            capabilities=_strings(value.get("capabilities")),
        )


@dataclass(frozen=True)
class ResourceCapability:
    dataset: str
    description: str
    route_decision: str
    temporal_contract: str
    markets: tuple[str, ...]
    required_history_start: Optional[str]
    frequency: str
    key_fields: tuple[str, ...]
    quality_threshold: str
    providers: tuple[ProviderRoute, ...]
    parent_job: str
    target_universe_owner: str
    transport: str
    checkpoint: str
    store: str
    watermark_domain: str
    read_api: str
    forward_owner: str
    historical_backfill_owner: Optional[str]
    probe_evidence: tuple[Mapping[str, Any], ...] = ()
    new_source_approved: bool = False
    standalone_job: bool = False
    rollout_enabled: bool = False
    limitations: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "description": self.description,
            "route_decision": self.route_decision,
            "temporal_contract": self.temporal_contract,
            "markets": list(self.markets),
            "required_history_start": self.required_history_start,
            "frequency": self.frequency,
            "key_fields": list(self.key_fields),
            "quality_threshold": self.quality_threshold,
            "providers": [
                {
                    "name": provider.name,
                    "existing": provider.existing,
                    "full_market": provider.full_market,
                    "capabilities": list(provider.capabilities),
                }
                for provider in self.providers
            ],
            "parent_job": self.parent_job,
            "target_universe_owner": self.target_universe_owner,
            "transport": self.transport,
            "checkpoint": self.checkpoint,
            "store": self.store,
            "watermark_domain": self.watermark_domain,
            "read_api": self.read_api,
            "forward_owner": self.forward_owner,
            "historical_backfill_owner": self.historical_backfill_owner,
            "probe_evidence": [dict(item) for item in self.probe_evidence],
            "new_source_approved": self.new_source_approved,
            "standalone_job": self.standalone_job,
            "rollout_enabled": self.rollout_enabled,
            "limitations": list(self.limitations),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ResourceCapability":
        dataset = str(value.get("dataset") or "").strip()
        if not dataset:
            raise CatalogValidationError("resource dataset is required")
        decision = str(value.get("route_decision") or "").strip()
        if decision not in ROUTE_DECISIONS:
            raise CatalogValidationError(
                f"{dataset}: unsupported route_decision {decision!r}"
            )
        temporal = str(value.get("temporal_contract") or "").strip()
        if temporal not in TEMPORAL_CONTRACTS:
            raise CatalogValidationError(
                f"{dataset}: unsupported temporal_contract {temporal!r}"
            )
        providers_raw = value.get("providers", [])
        if not isinstance(providers_raw, list):
            raise CatalogValidationError(f"{dataset}: providers must be a list")
        evidence_raw = value.get("probe_evidence", [])
        if not isinstance(evidence_raw, list):
            raise CatalogValidationError(f"{dataset}: probe_evidence must be a list")
        return cls(
            dataset=dataset,
            description=str(value.get("description") or dataset),
            route_decision=decision,
            temporal_contract=temporal,
            markets=_strings(value.get("markets")),
            required_history_start=(
                str(value["required_history_start"])
                if value.get("required_history_start")
                else None
            ),
            frequency=str(value.get("frequency") or "unknown"),
            key_fields=_strings(value.get("key_fields")),
            quality_threshold=str(value.get("quality_threshold") or "strict"),
            providers=tuple(ProviderRoute.from_mapping(item) for item in providers_raw),
            parent_job=str(value.get("parent_job") or ""),
            target_universe_owner=str(value.get("target_universe_owner") or ""),
            transport=str(value.get("transport") or ""),
            checkpoint=str(value.get("checkpoint") or ""),
            store=str(value.get("store") or ""),
            watermark_domain=str(value.get("watermark_domain") or ""),
            read_api=str(value.get("read_api") or ""),
            forward_owner=str(value.get("forward_owner") or ""),
            historical_backfill_owner=(
                str(value["historical_backfill_owner"])
                if value.get("historical_backfill_owner")
                else None
            ),
            probe_evidence=tuple(dict(item) for item in evidence_raw),
            new_source_approved=bool(value.get("new_source_approved", False)),
            standalone_job=bool(value.get("standalone_job", False)),
            rollout_enabled=bool(value.get("rollout_enabled", False)),
            limitations=_strings(value.get("limitations")),
        )

    @property
    def strict_pit_capable(self) -> bool:
        return self.temporal_contract == "knowledge_time_safe"

    def validate_admission(self) -> None:
        if not self.markets:
            raise CatalogValidationError(f"{self.dataset}: markets are required")
        if not self.key_fields:
            raise CatalogValidationError(f"{self.dataset}: key_fields are required")
        if self.store not in KNOWN_STORES:
            raise CatalogValidationError(
                f"{self.dataset}: unknown store owner {self.store!r}"
            )
        if self.parent_job not in KNOWN_PARENT_JOBS:
            if not (
                self.route_decision == "new_source_required"
                and self.new_source_approved
                and self.probe_evidence
            ):
                raise CatalogValidationError(
                    f"{self.dataset}: unknown parent job {self.parent_job!r}"
                )
        new_full_market = [
            provider.name
            for provider in self.providers
            if not provider.existing and provider.full_market
        ]
        if new_full_market and not (
            self.route_decision == "new_source_required"
            and self.new_source_approved
            and self.probe_evidence
        ):
            raise CatalogValidationError(
                f"{self.dataset}: new full-market providers require approved probe "
                f"evidence: {new_full_market}"
            )
        if self.standalone_job and not (
            self.route_decision == "new_source_required"
            and self.new_source_approved
            and self.probe_evidence
        ):
            raise CatalogValidationError(
                f"{self.dataset}: standalone job is blocked while an existing owner applies"
            )
        if self.route_decision == "new_source_required" and not self.probe_evidence:
            raise CatalogValidationError(
                f"{self.dataset}: new source decision requires bounded probe evidence"
            )
        if self.route_decision in {"reuse", "extend_existing"} and not any(
            provider.existing for provider in self.providers
        ):
            raise CatalogValidationError(
                f"{self.dataset}: {self.route_decision} requires an existing provider"
            )

    def public_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "description": self.description,
            "route_decision": self.route_decision,
            "temporal_contract": self.temporal_contract,
            "strict_pit_capable": self.strict_pit_capable,
            "markets": list(self.markets),
            "required_history_start": self.required_history_start,
            "frequency": self.frequency,
            "key_fields": list(self.key_fields),
            "quality_threshold": self.quality_threshold,
            "providers": [
                {
                    "name": item.name,
                    "existing": item.existing,
                    "full_market": item.full_market,
                    "capabilities": list(item.capabilities),
                }
                for item in self.providers
            ],
            "parent_job": self.parent_job,
            "target_universe_owner": self.target_universe_owner,
            "transport": self.transport,
            "checkpoint": self.checkpoint,
            "store": self.store,
            "watermark_domain": self.watermark_domain,
            "read_api": self.read_api,
            "forward_owner": self.forward_owner,
            "historical_backfill_owner": self.historical_backfill_owner,
            "rollout_enabled": self.rollout_enabled,
            "limitations": list(self.limitations),
        }


@dataclass(frozen=True)
class BacktestDataCatalog:
    schema_version: str
    catalog_version: str
    resources: tuple[ResourceCapability, ...]
    source_path: Optional[Path] = field(default=None, compare=False)

    @classmethod
    def load(cls, path: str | Path) -> "BacktestDataCatalog":
        resolved = Path(path)
        payload = json.loads(resolved.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise CatalogValidationError("resource catalog root must be an object")
        schema = str(payload.get("schema_version") or "")
        if schema != CATALOG_SCHEMA_VERSION:
            raise CatalogValidationError(
                f"unsupported resource catalog schema: {schema!r}"
            )
        resources_raw = payload.get("resources")
        if not isinstance(resources_raw, list) or not resources_raw:
            raise CatalogValidationError("resource catalog requires resources")
        catalog = cls(
            schema_version=schema,
            catalog_version=str(payload.get("catalog_version") or ""),
            resources=tuple(
                ResourceCapability.from_mapping(item) for item in resources_raw
            ),
            source_path=resolved,
        )
        catalog.validate()
        return catalog

    def validate(self) -> None:
        if not self.catalog_version:
            raise CatalogValidationError("catalog_version is required")
        datasets = [item.dataset for item in self.resources]
        duplicates = sorted({item for item in datasets if datasets.count(item) > 1})
        if duplicates:
            raise CatalogValidationError(f"duplicate resource datasets: {duplicates}")
        for resource in self.resources:
            resource.validate_admission()

    def get(self, dataset: str) -> ResourceCapability:
        for resource in self.resources:
            if resource.dataset == dataset:
                return resource
        raise KeyError(dataset)

    def public_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "catalog_version": self.catalog_version,
            "resources": [item.public_dict() for item in self.resources],
        }


class RuntimeReadinessAggregator:
    """Merge static route decisions with local, scope-specific coverage metrics."""

    def __init__(self, catalog: BacktestDataCatalog):
        self.catalog = catalog

    def aggregate(
        self,
        metrics: Mapping[str, Mapping[str, Any]],
        *,
        market: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        strict_pit: bool = True,
    ) -> dict[str, Any]:
        rows = []
        for resource in self.catalog.resources:
            local = dict(metrics.get(resource.dataset) or {})
            blockers = list(local.get("blockers") or [])
            if market and market not in resource.markets:
                blockers.append("unsupported_market")
            if resource.route_decision in {"unavailable", "manual_import_only"}:
                blockers.append(f"route_{resource.route_decision}")
            if strict_pit and not resource.strict_pit_capable:
                blockers.append(f"temporal_contract_{resource.temporal_contract}")
            covered_start = local.get("covered_start")
            covered_end = local.get("covered_end")
            if start_date and (not covered_start or str(covered_start) > start_date):
                blockers.append("coverage_start_gap")
            if end_date and (not covered_end or str(covered_end) < end_date):
                blockers.append("coverage_end_gap")
            unresolved = int(local.get("unresolved_count") or 0)
            if unresolved:
                blockers.append("unresolved_quality")
            unique_blockers = list(dict.fromkeys(blockers))
            rows.append(
                {
                    **resource.public_dict(),
                    "scope": {
                        "market": market,
                        "start_date": start_date,
                        "end_date": end_date,
                        "strict_pit": strict_pit,
                    },
                    "coverage": {
                        "target_count": int(local.get("target_count") or 0),
                        "covered_count": int(local.get("covered_count") or 0),
                        "covered_start": covered_start,
                        "covered_end": covered_end,
                        "unresolved_count": unresolved,
                        "latest_run": local.get("latest_run"),
                        "latest_watermark": int(local.get("latest_watermark") or 0),
                    },
                    "ready": not unique_blockers,
                    "blockers": unique_blockers,
                }
            )
        return {
            "catalog_version": self.catalog.catalog_version,
            "scope": {
                "market": market,
                "start_date": start_date,
                "end_date": end_date,
                "strict_pit": strict_pit,
            },
            "ready": bool(rows) and all(item["ready"] for item in rows),
            "resources": rows,
        }


def default_catalog_path() -> Path:
    return Path(__file__).resolve().parents[2] / "config" / "backtest_data_resources.json"


def load_default_catalog() -> BacktestDataCatalog:
    return BacktestDataCatalog.load(default_catalog_path())
