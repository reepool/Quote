"""Bounded, no-write capability probes for existing backtest data resources."""

from __future__ import annotations

import importlib
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional

from .catalog import BacktestDataCatalog


MAX_PROBE_IDENTIFIERS = 20
MAX_PROBE_DAYS = 31


def _date(value: str | date, *, field_name: str) -> date:
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must use YYYY-MM-DD") from exc


@dataclass(frozen=True)
class BoundedProbeScope:
    identifiers: tuple[str, ...]
    start_date: date
    end_date: date
    markets: tuple[str, ...] = ()

    @classmethod
    def build(
        cls,
        *,
        identifiers: Iterable[str],
        start_date: str | date,
        end_date: str | date,
        markets: Iterable[str] = (),
    ) -> "BoundedProbeScope":
        normalized_ids = tuple(
            dict.fromkeys(str(item).strip() for item in identifiers if str(item).strip())
        )
        if not normalized_ids:
            raise ValueError("probe identifiers are required")
        if len(normalized_ids) > MAX_PROBE_IDENTIFIERS:
            raise ValueError(
                f"probe identifiers must not exceed {MAX_PROBE_IDENTIFIERS}"
            )
        start = _date(start_date, field_name="start_date")
        end = _date(end_date, field_name="end_date")
        if end < start:
            raise ValueError("end_date must not be earlier than start_date")
        if (end - start).days + 1 > MAX_PROBE_DAYS:
            raise ValueError(f"probe date range must not exceed {MAX_PROBE_DAYS} days")
        return cls(
            identifiers=normalized_ids,
            start_date=start,
            end_date=end,
            markets=tuple(
                dict.fromkeys(str(item).strip().upper() for item in markets if str(item).strip())
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "identifiers": list(self.identifiers),
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "markets": list(self.markets),
            "bounded": True,
        }


@dataclass
class ProbeResult:
    dataset: str
    scope: BoundedProbeScope
    status: str = "success"
    request_count: int = 0
    returned_rows: int = 0
    would_write_count: int = 0
    fields: list[str] = field(default_factory=list)
    units: dict[str, str] = field(default_factory=dict)
    coverage_start: Optional[str] = None
    coverage_end: Optional[str] = None
    date_semantics: str = "unknown"
    temporal_contract: str = "unavailable"
    capabilities: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "scope": self.scope.as_dict(),
            "status": self.status,
            "no_write": True,
            "request_count": self.request_count,
            "returned_rows": self.returned_rows,
            "would_write_count": self.would_write_count,
            "fields": list(self.fields),
            "units": dict(self.units),
            "coverage_start": self.coverage_start,
            "coverage_end": self.coverage_end,
            "date_semantics": self.date_semantics,
            "temporal_contract": self.temporal_contract,
            "capabilities": dict(self.capabilities),
            "errors": list(self.errors),
        }


class ReadOnlySQLiteInspector:
    """Inspect schema and bounded counts through SQLite query-only mode."""

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def inspect(self, tables: Iterable[str]) -> dict[str, Any]:
        requested = tuple(dict.fromkeys(str(item) for item in tables))
        if not self.path.exists():
            return {
                "database": str(self.path),
                "exists": False,
                "tables": {item: {"exists": False, "columns": [], "rows": 0} for item in requested},
            }
        connection = sqlite3.connect(
            f"file:{self.path.resolve()}?mode=ro",
            uri=True,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA query_only=ON")
            result: dict[str, Any] = {}
            for table in requested:
                if not table.replace("_", "").isalnum():
                    raise ValueError(f"invalid table name: {table}")
                exists = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
                    (table,),
                ).fetchone()
                if exists is None:
                    result[table] = {"exists": False, "columns": [], "rows": 0}
                    continue
                columns = [
                    str(row["name"])
                    for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
                ]
                rows = int(connection.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()["count"])
                result[table] = {"exists": True, "columns": columns, "rows": rows}
            return {"database": str(self.path), "exists": True, "tables": result}
        finally:
            connection.close()


def _callables(module_name: str, names: Iterable[str]) -> dict[str, bool]:
    try:
        module = importlib.import_module(module_name)
    except Exception:
        return {str(name): False for name in names}
    return {str(name): callable(getattr(module, str(name), None)) for name in names}


class ExistingResourceProbeSuite:
    """Audit installed provider surfaces without issuing remote requests."""

    def __init__(
        self,
        *,
        catalog: BacktestDataCatalog,
        quotes_db_path: str | Path,
        financials_db_path: str | Path,
        research_db_path: str | Path,
    ) -> None:
        self.catalog = catalog
        self.quotes = ReadOnlySQLiteInspector(quotes_db_path)
        self.financials = ReadOnlySQLiteInspector(financials_db_path)
        self.research = ReadOnlySQLiteInspector(research_db_path)

    def run(self, dataset: str, scope: BoundedProbeScope) -> ProbeResult:
        handlers: Mapping[str, Callable[[BoundedProbeScope], ProbeResult]] = {
            "index_composition": self.probe_index_composition,
            "security_state": self.probe_security_state,
            "daily_price_limits": self.probe_daily_price_limits,
            "financial_filing_vintages": self.probe_financial_vintages,
            "canonical_corporate_actions": self.probe_corporate_actions,
            "industry_membership": self.probe_industry_membership,
            "industry_returns": self.probe_industry_returns,
        }
        if dataset not in handlers:
            raise ValueError(f"unsupported probe dataset: {dataset}")
        return handlers[dataset](scope)

    def run_all(self, scope: BoundedProbeScope) -> dict[str, Any]:
        results = [self.run(item.dataset, scope).as_dict() for item in self.catalog.resources]
        return {
            "catalog_version": self.catalog.catalog_version,
            "scope": scope.as_dict(),
            "no_write": True,
            "results": results,
            "route_decisions": {
                item.dataset: {
                    "route_decision": item.route_decision,
                    "forward_owner": item.forward_owner,
                    "historical_backfill_owner": item.historical_backfill_owner,
                    "production_history_enabled": bool(
                        item.rollout_enabled
                        and item.historical_backfill_owner
                        and item.temporal_contract == "knowledge_time_safe"
                    ),
                }
                for item in self.catalog.resources
            },
        }

    def probe_index_composition(self, scope: BoundedProbeScope) -> ProbeResult:
        routes = _callables(
            "akshare",
            ("index_stock_cons", "index_stock_cons_csindex", "index_stock_cons_weight_csindex"),
        )
        official = _callables(
            "data_sources.official_index_source",
            ("CNIndexSource", "CSIndexSource"),
        )
        schema = self.quotes.inspect(("instruments", "index_composition_snapshots"))
        return ProbeResult(
            dataset="index_composition",
            scope=scope,
            temporal_contract="current_only",
            date_semantics="installed AkShare constituent routes are treated as current-only until a bounded response proves otherwise",
            capabilities={"akshare": routes, "official": official, "schema": schema},
            fields=["constituent_symbol", "weight", "reference_date"],
            units={"weight": "provider_declared_or_unknown"},
        )

    def probe_security_state(self, scope: BoundedProbeScope) -> ProbeResult:
        routes = _callables(
            "akshare",
            ("stock_zh_a_st_em", "stock_info_sh_delist", "stock_info_sz_delist"),
        )
        schema = self.quotes.inspect(
            ("instruments", "corporate_action_document_artifacts", "security_state_events")
        )
        return ProbeResult(
            dataset="security_state",
            scope=scope,
            temporal_contract="knowledge_time_safe",
            date_semantics="current master is forward-only; official announcements carry publication and effective dates",
            capabilities={"akshare": routes, "schema": schema},
            fields=["event_type", "effective_date", "published_at", "available_at"],
        )

    def probe_daily_price_limits(self, scope: BoundedProbeScope) -> ProbeResult:
        routes = _callables(
            "akshare",
            ("stock_zt_pool_em", "stock_zt_pool_dtgc_em"),
        )
        schema = self.quotes.inspect(("daily_quotes", "daily_price_limit_revisions"))
        quote_columns = set(
            (schema.get("tables") or {}).get("daily_quotes", {}).get("columns") or []
        )
        reported = {"limit_up", "limit_down"}.issubset(quote_columns)
        return ProbeResult(
            dataset="daily_price_limits",
            scope=scope,
            temporal_contract="knowledge_time_safe" if reported else "unavailable",
            date_semantics="limit-hit pools validate hits but do not represent all-market reference coverage",
            capabilities={
                "akshare_hit_pools": routes,
                "quote_has_complete_reference_fields": reported,
                "schema": schema,
            },
            fields=["limit_up", "limit_down", "reference_price", "trade_date"],
            units={"price": "CNY_per_share"},
        )

    def probe_financial_vintages(self, scope: BoundedProbeScope) -> ProbeResult:
        schema = self.financials.inspect(
            (
                "financial_source_files",
                "financial_numeric_facts_hot",
                "financial_numeric_facts_history",
                "financial_filing_relationship_decisions",
                "financial_parse_revisions",
            )
        )
        source = (schema.get("tables") or {}).get("financial_source_files", {})
        columns = set(source.get("columns") or [])
        required = {"source_file_id", "content_hash", "published_at", "filing_id"}
        return ProbeResult(
            dataset="financial_filing_vintages",
            scope=scope,
            returned_rows=int(source.get("rows") or 0),
            temporal_contract=(
                "knowledge_time_safe" if required.issubset(columns) else "unavailable"
            ),
            date_semantics="published_at is source publication; parsed availability is retained separately",
            capabilities={"required_source_fields_present": required.issubset(columns), "schema": schema},
            fields=sorted(required | {"available_at", "parse_revision_id", "period_semantic"}),
        )

    def probe_corporate_actions(self, scope: BoundedProbeScope) -> ProbeResult:
        schema = self.quotes.inspect(
            (
                "corporate_action_observations",
                "corporate_action_effective_date_evidence",
                "corporate_action_resolved_terms",
                "corporate_action_resolution_states",
                "canonical_corporate_action_revisions",
            )
        )
        observations = (schema.get("tables") or {}).get("corporate_action_observations", {})
        return ProbeResult(
            dataset="canonical_corporate_actions",
            scope=scope,
            returned_rows=int(observations.get("rows") or 0),
            temporal_contract="knowledge_time_safe",
            date_semantics="source evidence and projection decision availability are distinct",
            capabilities={"schema": schema, "acquisition_reused": True},
            fields=["event_type", "effective_date", "terms", "backtest_ready", "available_at"],
        )

    def probe_industry_membership(self, scope: BoundedProbeScope) -> ProbeResult:
        schema = self.research.inspect(("industry_classification_history", "industry_memberships"))
        history = (schema.get("tables") or {}).get("industry_classification_history", {})
        columns = set(history.get("columns") or [])
        knowledge_time = "available_at" in columns and "revision_id" in columns
        return ProbeResult(
            dataset="industry_membership",
            scope=scope,
            returned_rows=int(history.get("rows") or 0),
            temporal_contract="knowledge_time_safe" if knowledge_time else "effective_date_only",
            date_semantics="official_start_date is effective time; current storage does not retain knowledge-time revisions",
            capabilities={"schema": schema, "knowledge_time_lineage": knowledge_time},
            fields=["official_start_date", "official_industry_code", "taxonomy_version"],
        )

    def probe_industry_returns(self, scope: BoundedProbeScope) -> ProbeResult:
        schema = self.research.inspect(("industry_index_analysis_daily",))
        return ProbeResult(
            dataset="industry_returns",
            scope=scope,
            temporal_contract="knowledge_time_safe",
            date_semantics="trading-day return observation",
            capabilities={"schema": schema, "existing_api": "/api/v1/research/industry/index-analysis"},
            fields=["industry_code", "trade_date", "return"],
            units={"return": "decimal_or_source_declared"},
        )
