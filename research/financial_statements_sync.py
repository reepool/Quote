"""
Financial statements shadow sync service.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from datetime import date
from typing import Any, Dict, List, Optional

from research.empty_support import allows_optional_empty_exchange
from research.financial_fact_aliases import get_core_financial_fact_aliases
from research.financial_xbrl_parser import (
    FinancialSseStructuredJsonFactParser,
    FinancialStructuredFilingParserDispatcher,
    FinancialXbrlNumericFactParser,
)
from research.providers import (
    FinancialStatementsProviderRegistry,
    OfficialFinancialFilingProviderRegistry,
)
from research.providers.base import (
    FinancialFilingPayload,
    FinancialSourceFileManifest,
    FinancialStatementBundle,
)
from research.source_policy import ResearchSourcePolicyResolver
from research.storage import ResearchStorageManager
from utils import dm_logger
from utils.config_manager import ResearchConfig, config_manager
from utils.date_utils import get_shanghai_time


_QUARTER_ENDS = {
    1: (3, 31),
    2: (6, 30),
    3: (9, 30),
    4: (12, 31),
}


@dataclass(frozen=True)
class FinancialStatementsExchangeSyncResult:
    """Per-exchange result for financial statements shadow sync."""

    exchange: str
    status: str
    source: Optional[str] = None
    mode: Optional[str] = None
    attempted_sources: List[str] = field(default_factory=list)
    report_periods_attempted: List[str] = field(default_factory=list)
    bundles_written: int = 0
    raw_rows_written: int = 0
    source_manifests_written: int = 0
    official_payloads_processed: int = 0
    numeric_facts_written: int = 0
    core_facts_written: int = 0
    unchanged_files_skipped: int = 0
    tier_maintenance: Optional[Dict[str, Any]] = None
    coverage_gaps: Optional[Dict[str, Any]] = None
    official_fallback_reasons: List[str] = field(default_factory=list)
    error_message: Optional[str] = None


def build_financial_report_periods(
    *,
    baseline_report_period: str = "2024Q1",
    rolling_min_quarters: int = 8,
    latest_report_period: Optional[str] = None,
    optional_anchor_period: Optional[str] = None,
    include_optional_anchor: bool = False,
    today: Optional[date] = None,
) -> List[str]:
    """Build quarter-end report periods without assuming future disclosure."""
    latest_period_date = (
        _parse_report_period(latest_report_period)
        if latest_report_period
        else _latest_disclosed_report_period(today or get_shanghai_time().date())
    )
    baseline_date = _parse_report_period(baseline_report_period)

    start_year, start_quarter = _date_to_quarter(baseline_date)
    end_year, end_quarter = _date_to_quarter(latest_period_date)

    periods: List[str] = []
    cursor_year, cursor_quarter = start_year, start_quarter
    while (cursor_year, cursor_quarter) <= (end_year, end_quarter):
        periods.append(_format_quarter_period(cursor_year, cursor_quarter))
        cursor_year, cursor_quarter = _add_quarters(cursor_year, cursor_quarter, 1)

    while len(periods) < max(1, int(rolling_min_quarters or 1)):
        first_year, first_quarter = _date_to_quarter(_parse_report_period(periods[0]))
        prev_year, prev_quarter = _add_quarters(first_year, first_quarter, -1)
        periods.insert(0, _format_quarter_period(prev_year, prev_quarter))

    if include_optional_anchor and optional_anchor_period:
        anchor = _parse_report_period(optional_anchor_period).isoformat()
        if anchor not in periods:
            periods.append(anchor)

    return sorted(set(periods))


class FinancialStatementsShadowSyncService:
    """Run financial statements shadow sync into research.db."""

    def __init__(
        self,
        *,
        db_ops: Any,
        storage: ResearchStorageManager,
        research_config: Optional[ResearchConfig] = None,
        resolver: Optional[ResearchSourcePolicyResolver] = None,
        registry: Optional[FinancialStatementsProviderRegistry] = None,
        official_registry: Optional[OfficialFinancialFilingProviderRegistry] = None,
        numeric_fact_parser: Optional[Any] = None,
    ):
        self.db_ops = db_ops
        self.storage = storage
        self.research_config = research_config or config_manager.get_research_config()
        self.resolver = resolver or ResearchSourcePolicyResolver(self.research_config)
        self.registry = registry or FinancialStatementsProviderRegistry(
            research_config=self.research_config,
        )
        self.official_registry = official_registry or OfficialFinancialFilingProviderRegistry(
            research_config=self.research_config,
        )
        parser_cfg = self._module_config().get("parser", {})
        parser_version = str(
            parser_cfg.get("parser_version", "financial_structured_filing.v1")
        )
        numeric_parser_version = str(
            parser_cfg.get("numeric_fact_parser", "xbrl_numeric_facts.v1")
        )
        structured_json_parser_version = str(
            parser_cfg.get(
                "structured_json_fact_parser",
                "sse_commonquery_structured_json_facts.v1",
            )
        )
        self.numeric_fact_parser = numeric_fact_parser or FinancialStructuredFilingParserDispatcher(
            parser_version=parser_version,
            xbrl_parser=FinancialXbrlNumericFactParser(
                parser_version=numeric_parser_version,
            ),
            structured_json_parser=FinancialSseStructuredJsonFactParser(
                parser_version=structured_json_parser_version,
            ),
        )

    async def sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        report_periods: Optional[List[str]] = None,
        sync_mode: str = "backfill",
        force_full: bool = False,
    ) -> Dict[str, Any]:
        target_exchanges = exchanges or self.research_config.markets
        target_periods = (
            self._normalize_report_periods(report_periods)
            if report_periods
            else self._configured_report_periods()
        )
        results: List[FinancialStatementsExchangeSyncResult] = []

        for exchange in target_exchanges:
            results.append(
                await self._sync_exchange(
                    exchange=exchange,
                    limit_per_exchange=limit_per_exchange,
                    budget_mode=budget_mode,
                    allow_paid_proxy=allow_paid_proxy,
                    report_periods=target_periods,
                    sync_mode=sync_mode,
                    force_full=force_full,
                )
            )

        total_bundles_written = sum(result.bundles_written for result in results)
        total_raw_rows_written = sum(result.raw_rows_written for result in results)
        total_numeric_facts_written = sum(
            result.numeric_facts_written for result in results
        )
        total_core_facts_written = sum(result.core_facts_written for result in results)
        total_manifests_written = sum(
            result.source_manifests_written for result in results
        )
        total_unchanged_skipped = sum(
            result.unchanged_files_skipped for result in results
        )
        success_count = sum(1 for result in results if result.status == "success")

        return {
            "status": "success" if success_count else "degraded",
            "sync_mode": sync_mode,
            "report_periods": target_periods,
            "exchanges": [asdict(result) for result in results],
            "total_bundles_written": total_bundles_written,
            "total_raw_rows_written": total_raw_rows_written,
            "total_source_manifests_written": total_manifests_written,
            "total_numeric_facts_written": total_numeric_facts_written,
            "total_core_facts_written": total_core_facts_written,
            "total_unchanged_files_skipped": total_unchanged_skipped,
            "successful_exchanges": success_count,
            "attempted_exchanges": len(results),
        }

    async def _sync_exchange(
        self,
        *,
        exchange: str,
        limit_per_exchange: Optional[int],
        budget_mode: Optional[str],
        allow_paid_proxy: Optional[bool],
        report_periods: List[str],
        sync_mode: str,
        force_full: bool,
    ) -> FinancialStatementsExchangeSyncResult:
        instruments = await self.db_ops.get_instruments_by_exchange(exchange)
        stock_instruments = [
            instrument
            for instrument in instruments
            if instrument.get("type") == "stock" and instrument.get("is_active", True)
        ]

        if limit_per_exchange is not None:
            stock_instruments = stock_instruments[:limit_per_exchange]

        if not stock_instruments:
            return FinancialStatementsExchangeSyncResult(
                exchange=exchange,
                status="skipped",
                report_periods_attempted=report_periods,
                error_message="No active stock instruments found for exchange",
            )

        checkpoint = self.storage.get_latest_successful_ingestion_run(
            domain="financial_statements",
            job_name="financial_statements_shadow_sync",
            market=exchange,
        )
        run_id = self.storage.start_ingestion_run(
            domain="financial_statements",
            job_name="financial_statements_shadow_sync",
            market=exchange,
            metadata={
                "instrument_count": len(stock_instruments),
                "report_periods": report_periods,
                "sync_mode": sync_mode,
                "force_full": force_full,
                "previous_checkpoint": checkpoint,
            },
        )

        attempted_sources: List[str] = []
        optional_empty_exchange = allows_optional_empty_exchange(
            self.research_config,
            "financial_statements",
            exchange,
        )
        try:
            plan = self.resolver.resolve(
                "financial_statements",
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
            )
            official_fallback_reasons: List[str] = []

            for candidate in plan.candidates:
                attempted_sources.append(f"{candidate.source}:{candidate.mode}")

                official_provider = self.official_registry.get(candidate.source)
                if official_provider is not None and official_provider.supports_mode(
                    candidate.mode
                ):
                    official_result = await self._process_official_candidate(
                        source=candidate.source,
                        mode=candidate.mode,
                        provider=official_provider,
                        instruments=stock_instruments,
                        exchange=exchange,
                        report_periods=report_periods,
                        limit_per_exchange=limit_per_exchange,
                        run_id=run_id,
                        sync_mode=sync_mode,
                        force_full=force_full,
                    )
                    if official_result is not None and (
                        official_result.numeric_facts_written
                        or official_result.core_facts_written
                        or official_result.unchanged_files_skipped
                    ):
                        result = self._finalize_successful_exchange(
                            run_id=run_id,
                            exchange=exchange,
                            source=candidate.source,
                            mode=candidate.mode,
                            attempted_sources=attempted_sources,
                            report_periods=report_periods,
                            stock_instruments=stock_instruments,
                            sync_mode=sync_mode,
                            source_manifests_written=official_result.source_manifests_written,
                            official_payloads_processed=official_result.official_payloads_processed,
                            numeric_facts_written=official_result.numeric_facts_written,
                            core_facts_written=official_result.core_facts_written,
                            unchanged_files_skipped=official_result.unchanged_files_skipped,
                            official_fallback_reasons=official_fallback_reasons,
                        )
                        return result
                    reason = (
                        official_result.error_message
                        if official_result is not None
                        else "no_official_payloads_or_provider_error"
                    )
                    official_fallback_reasons.append(
                        f"{candidate.source}:{candidate.mode}:{reason}"
                    )
                    continue

                provider = self.registry.get(candidate.source)
                if provider is None or not provider.supports_mode(candidate.mode):
                    continue

                try:
                    bundles = await provider.fetch_financial_statement_bundles(
                        instruments=stock_instruments,
                        exchange=exchange,
                        mode=candidate.mode,
                        limit=limit_per_exchange,
                        report_periods=report_periods,
                    )
                except TypeError as e:
                    if "report_periods" not in str(e):
                        raise
                    bundles = await provider.fetch_financial_statement_bundles(
                        instruments=stock_instruments,
                        exchange=exchange,
                        mode=candidate.mode,
                        limit=limit_per_exchange,
                    )
                except Exception as e:
                    dm_logger.warning(
                        "[FinancialStatementsSync] Provider %s (%s) failed for %s: %s",
                        candidate.source,
                        candidate.mode,
                        exchange,
                        e,
                    )
                    continue

                filtered_bundles = [
                    bundle for bundle in bundles if bundle.report_period in report_periods
                ]
                if not filtered_bundles:
                    continue

                fallback_result = self._write_fallback_bundles(
                    bundles=filtered_bundles,
                    source=candidate.source,
                    mode=candidate.mode,
                    run_id=run_id,
                    sync_mode=sync_mode,
                    force_full=force_full,
                )
                result = self._finalize_successful_exchange(
                    run_id=run_id,
                    exchange=exchange,
                    source=candidate.source,
                    mode=candidate.mode,
                    attempted_sources=attempted_sources,
                    report_periods=report_periods,
                    stock_instruments=stock_instruments,
                    sync_mode=sync_mode,
                    bundles_written=fallback_result["bundles_written"],
                    raw_rows_written=fallback_result["raw_rows_written"],
                    source_manifests_written=fallback_result[
                        "source_manifests_written"
                    ],
                    core_facts_written=fallback_result["core_facts_written"],
                    unchanged_files_skipped=fallback_result[
                        "unchanged_files_skipped"
                    ],
                    official_fallback_reasons=official_fallback_reasons,
                )
                return result

            self.storage.finish_ingestion_run(
                run_id,
                status="success" if optional_empty_exchange else "degraded",
                rows_written=0,
                error_message=None
                if optional_empty_exchange
                else "No provider returned financial statement bundles",
                metadata={
                    "exchange": exchange,
                    "attempted_sources": attempted_sources,
                    "optional_empty_exchange": optional_empty_exchange,
                    "report_periods": report_periods,
                    "sync_mode": sync_mode,
                    "previous_checkpoint": checkpoint,
                },
            )
            return FinancialStatementsExchangeSyncResult(
                exchange=exchange,
                status="success" if optional_empty_exchange else "degraded",
                attempted_sources=attempted_sources,
                report_periods_attempted=report_periods,
                error_message=None
                if optional_empty_exchange
                else "No provider returned financial statement bundles",
            )
        except Exception as e:
            self.storage.finish_ingestion_run(
                run_id,
                status="failed",
                rows_written=0,
                error_message=str(e),
                metadata={
                    "exchange": exchange,
                    "attempted_sources": attempted_sources,
                    "report_periods": report_periods,
                    "sync_mode": sync_mode,
                },
            )
            return FinancialStatementsExchangeSyncResult(
                exchange=exchange,
                status="failed",
                attempted_sources=attempted_sources,
                report_periods_attempted=report_periods,
                error_message=str(e),
            )

    async def _process_official_candidate(
        self,
        *,
        source: str,
        mode: str,
        provider: Any,
        instruments: List[Dict[str, Any]],
        exchange: str,
        report_periods: List[str],
        limit_per_exchange: Optional[int],
        run_id: int,
        sync_mode: str,
        force_full: bool,
    ) -> Optional[FinancialStatementsExchangeSyncResult]:
        try:
            payloads = await provider.fetch_financial_filings(
                instruments=instruments,
                exchange=exchange,
                report_periods=report_periods,
                mode=mode,
                limit=limit_per_exchange,
            )
        except Exception as e:
            dm_logger.warning(
                "[FinancialStatementsSync] Official provider %s (%s) failed for %s: %s",
                source,
                mode,
                exchange,
                e,
            )
            return None
        if not payloads:
            return None

        manifests_written = 0
        payloads_processed = 0
        numeric_written = 0
        core_written = 0
        unchanged_skipped = 0

        for payload in payloads:
            if (
                sync_mode == "catchup"
                and not force_full
                and self._has_unchanged_manifest_with_core(payload.manifest)
            ):
                unchanged_skipped += 1
                continue
            counts = self._write_official_payload(payload, run_id=run_id)
            manifests_written += counts["source_manifests_written"]
            payloads_processed += counts["official_payloads_processed"]
            numeric_written += counts["numeric_facts_written"]
            core_written += counts["core_facts_written"]

        return FinancialStatementsExchangeSyncResult(
            exchange=exchange,
            status="success" if payloads_processed or unchanged_skipped else "degraded",
            source=source,
            mode=mode,
            report_periods_attempted=report_periods,
            source_manifests_written=manifests_written,
            official_payloads_processed=payloads_processed,
            numeric_facts_written=numeric_written,
            core_facts_written=core_written,
            unchanged_files_skipped=unchanged_skipped,
            error_message=None
            if numeric_written or core_written or unchanged_skipped
            else "official_payloads_unparseable_or_no_core_facts",
        )

    def _write_official_payload(
        self,
        payload: FinancialFilingPayload,
        *,
        run_id: int,
    ) -> Dict[str, int]:
        manifest = payload.manifest
        source_file_id = self.storage.financial_statements.upsert_source_file_manifest(
            manifest,
            ingestion_run_id=run_id,
        )
        manifests_written = 1
        try:
            parse_payload = payload.text if payload.text is not None else payload.content
            parse_kwargs = {
                "source_file_id": source_file_id,
                "instrument_id": str(manifest.instrument_id or ""),
                "symbol": str(manifest.symbol or ""),
                "exchange": manifest.exchange,
                "report_period": manifest.report_period,
                "source": manifest.source,
                "source_mode": manifest.source_mode,
                "report_type": manifest.report_type,
            }
            artifact_kind = (manifest.metadata_json or {}).get("artifact_kind")
            if artifact_kind:
                parse_kwargs["artifact_kind"] = artifact_kind
            if payload.content_type:
                parse_kwargs["content_type"] = payload.content_type
            try:
                parse_result = self.numeric_fact_parser.parse(
                    parse_payload,
                    **parse_kwargs,
                )
            except TypeError as type_error:
                if "unexpected keyword argument" not in str(type_error):
                    raise
                parse_kwargs.pop("artifact_kind", None)
                parse_kwargs.pop("content_type", None)
                parse_result = self.numeric_fact_parser.parse(
                    parse_payload,
                    **parse_kwargs,
                )
        except Exception as e:
            failed_manifest = replace(
                manifest,
                source_file_id=source_file_id,
                status="parse_failed",
                parser_diagnostics={"error": str(e)},
            )
            self.storage.financial_statements.upsert_source_file_manifest(
                failed_manifest,
                ingestion_run_id=run_id,
            )
            return {
                "source_manifests_written": manifests_written,
                "official_payloads_processed": 1,
                "numeric_facts_written": 0,
                "core_facts_written": 0,
            }

        numeric_written = self.storage.financial_statements.upsert_numeric_facts(
            parse_result.numeric_facts,
            ingestion_run_id=run_id,
        )
        core_written = 0
        core = self.storage.derive_financial_core_facts_from_numeric_facts(
            str(manifest.instrument_id or ""),
            manifest.report_period,
            alias_mapping=self._core_fact_alias_mapping(),
        )
        if core is not None:
            core = replace(
                core,
                publish_date=manifest.published_at,
                data_available_date=manifest.published_at,
                report_type=manifest.report_type,
                source_file_id=source_file_id,
                filing_id=manifest.filing_id,
                lineage_json={
                    **core.lineage_json,
                    "source_file_id": source_file_id,
                    "filing_id": manifest.filing_id,
                    "parser_diagnostics": parse_result.diagnostics,
                },
            )
            self.storage.upsert_financial_facts(core, ingestion_run_id=run_id)
            core_written = 1

        parsed_manifest = replace(
            manifest,
            source_file_id=source_file_id,
            status="parsed",
            parser_diagnostics=parse_result.diagnostics,
        )
        self.storage.financial_statements.upsert_source_file_manifest(
            parsed_manifest,
            ingestion_run_id=run_id,
        )
        return {
            "source_manifests_written": manifests_written,
            "official_payloads_processed": 1,
            "numeric_facts_written": numeric_written,
            "core_facts_written": core_written,
        }

    def _write_fallback_bundles(
        self,
        *,
        bundles: List[FinancialStatementBundle],
        source: str,
        mode: str,
        run_id: int,
        sync_mode: str,
        force_full: bool,
    ) -> Dict[str, int]:
        bundles_written = 0
        raw_rows_written = 0
        manifests_written = 0
        core_facts_written = 0
        unchanged_skipped = 0

        for bundle in bundles:
            payload_hash = self._hash_payload(bundle.raw_payload)
            manifest = FinancialSourceFileManifest(
                source=source,
                source_mode=mode,
                instrument_id=bundle.instrument_id,
                symbol=bundle.symbol,
                exchange=bundle.exchange,
                report_period=bundle.report_period,
                report_type=self._report_type_for_period(bundle.report_period),
                content_hash=payload_hash,
                parser_version=self._fallback_parser_version(source),
                status="parsed",
                parser_diagnostics={"fallback_bundle": True},
                metadata_json={"source_payload_schema": "financial_statement_bundle"},
            )
            if (
                sync_mode == "catchup"
                and not force_full
                and self._has_unchanged_manifest_with_core(manifest)
            ):
                unchanged_skipped += 1
                continue

            source_file_id = self.storage.financial_statements.upsert_source_file_manifest(
                manifest,
                ingestion_run_id=run_id,
            )
            bundle_to_write = self._attach_source_file_to_bundle(
                bundle,
                source_file_id=source_file_id,
                payload_hash=payload_hash,
            )
            self.storage.upsert_financial_statement_bundle(
                bundle_to_write,
                ingestion_run_id=run_id,
            )
            self.storage.store_raw_payload(
                domain="financial_statements",
                instrument_id=bundle.instrument_id,
                source=bundle.source,
                source_mode=bundle.source_mode,
                payload=bundle.raw_payload,
                payload_hash=payload_hash,
                ingestion_run_id=run_id,
            )
            manifests_written += 1
            bundles_written += 1
            raw_rows_written += len(bundle.raw_statements)
            if bundle.facts is not None:
                core_facts_written += 1

        return {
            "bundles_written": bundles_written,
            "raw_rows_written": raw_rows_written,
            "source_manifests_written": manifests_written,
            "core_facts_written": core_facts_written,
            "unchanged_files_skipped": unchanged_skipped,
        }

    def _finalize_successful_exchange(
        self,
        *,
        run_id: int,
        exchange: str,
        source: str,
        mode: str,
        attempted_sources: List[str],
        report_periods: List[str],
        stock_instruments: List[Dict[str, Any]],
        sync_mode: str,
        bundles_written: int = 0,
        raw_rows_written: int = 0,
        source_manifests_written: int = 0,
        official_payloads_processed: int = 0,
        numeric_facts_written: int = 0,
        core_facts_written: int = 0,
        unchanged_files_skipped: int = 0,
        official_fallback_reasons: Optional[List[str]] = None,
    ) -> FinancialStatementsExchangeSyncResult:
        official_fallback_reasons = official_fallback_reasons or []
        tier_result = self._run_tier_maintenance_if_enabled()
        coverage_gaps = self._build_coverage_gaps(
            instruments=stock_instruments,
            report_periods=report_periods,
        )
        rows_written = (
            bundles_written
            + raw_rows_written
            + source_manifests_written
            + official_payloads_processed
            + numeric_facts_written
            + core_facts_written
        )
        self.storage.finish_ingestion_run(
            run_id,
            status="success",
            rows_written=rows_written,
            metadata={
                "exchange": exchange,
                "source": source,
                "mode": mode,
                "attempted_sources": attempted_sources,
                "report_periods": report_periods,
                "sync_mode": sync_mode,
                "checkpoint": {
                    "completed_at": get_shanghai_time().isoformat(),
                    "source": source,
                    "mode": mode,
                    "report_periods": report_periods,
                },
                "counts": {
                    "bundles_written": bundles_written,
                    "raw_rows_written": raw_rows_written,
                    "source_manifests_written": source_manifests_written,
                    "official_payloads_processed": official_payloads_processed,
                    "numeric_facts_written": numeric_facts_written,
                    "core_facts_written": core_facts_written,
                    "unchanged_files_skipped": unchanged_files_skipped,
                },
                "tier_maintenance": tier_result,
                "coverage_gaps": coverage_gaps,
                "official_fallback_reasons": official_fallback_reasons,
            },
        )
        return FinancialStatementsExchangeSyncResult(
            exchange=exchange,
            status="success",
            source=source,
            mode=mode,
            attempted_sources=attempted_sources,
            report_periods_attempted=report_periods,
            bundles_written=bundles_written,
            raw_rows_written=raw_rows_written,
            source_manifests_written=source_manifests_written,
            official_payloads_processed=official_payloads_processed,
            numeric_facts_written=numeric_facts_written,
            core_facts_written=core_facts_written,
            unchanged_files_skipped=unchanged_files_skipped,
            tier_maintenance=tier_result,
            coverage_gaps=coverage_gaps,
            official_fallback_reasons=official_fallback_reasons,
        )

    def _build_coverage_gaps(
        self,
        *,
        instruments: List[Dict[str, Any]],
        report_periods: List[str],
    ) -> Dict[str, Any]:
        required_core_facts = list(
            self._module_config()
            .get("readiness", {})
            .get(
                "required_core_facts",
                ["revenue", "net_income", "equity", "total_assets", "total_liabilities"],
            )
        )
        instrument_ids = [
            str(instrument.get("instrument_id"))
            for instrument in instruments
            if instrument.get("instrument_id")
        ]
        return self.storage.detect_financial_coverage_gaps(
            expected_periods=report_periods,
            instrument_ids=instrument_ids,
            required_core_facts=required_core_facts,
            fallback_sources=list(
                self._module_config()
                .get("fallback_policy", {})
                .get("fallback_source_priority", ["akshare"])
            ),
        )

    def _run_tier_maintenance_if_enabled(self) -> Optional[Dict[str, Any]]:
        storage_cfg = self._module_config().get("storage", {})
        tier_cfg = storage_cfg.get("tier_maintenance", {})
        if not bool(tier_cfg.get("enabled", True)) or not bool(
            tier_cfg.get("run_after_successful_sync", True)
        ):
            return None
        return self.storage.financial_statements.maintain_tiers(
            hot_quarter_window=int(storage_cfg.get("hot_quarter_window", 12)),
        )

    def _has_unchanged_manifest_with_core(
        self,
        manifest: FinancialSourceFileManifest,
    ) -> bool:
        if not manifest.content_hash or not manifest.instrument_id:
            return False
        manifests = self.storage.get_financial_source_file_manifests(
            instrument_id=manifest.instrument_id,
            report_period=manifest.report_period,
            source=manifest.source,
        )
        unchanged = any(
            row.get("content_hash") == manifest.content_hash
            and row.get("status") in {"downloaded", "parsed"}
            for row in manifests
        )
        if not unchanged:
            return False
        return bool(
            self.storage.get_financial_core_facts(
                manifest.instrument_id,
                include_history=True,
                report_period=manifest.report_period,
                limit=1,
            )
        )

    @staticmethod
    def _attach_source_file_to_bundle(
        bundle: FinancialStatementBundle,
        *,
        source_file_id: str,
        payload_hash: str,
    ) -> FinancialStatementBundle:
        if bundle.facts is None:
            return bundle
        facts = replace(
            bundle.facts,
            source_file_id=source_file_id,
            report_type=bundle.facts.report_type
            or _report_type_for_period(bundle.report_period),
            data_available_date=bundle.facts.data_available_date
            or bundle.facts.publish_date,
            lineage_json={
                **bundle.facts.lineage_json,
                "source_file_id": source_file_id,
                "payload_hash": payload_hash,
            },
        )
        return replace(bundle, facts=facts)

    def _configured_report_periods(self) -> List[str]:
        module_cfg = self._module_config()
        history_cfg = module_cfg.get("history", {})
        storage_cfg = module_cfg.get("storage", {})
        hot_anchor_policy = storage_cfg.get("hot_anchor_policy", {})
        return build_financial_report_periods(
            baseline_report_period=str(
                history_cfg.get("baseline_report_period", "2024Q1")
            ),
            rolling_min_quarters=int(history_cfg.get("rolling_min_quarters", 8)),
            optional_anchor_period=history_cfg.get("optional_ttm_anchor_period"),
            include_optional_anchor=bool(
                hot_anchor_policy.get("include_ttm_anchor_period", False)
            ),
        )

    @staticmethod
    def _normalize_report_periods(report_periods: List[str]) -> List[str]:
        return sorted({_parse_report_period(period).isoformat() for period in report_periods})

    def _alias_mapping_version(self) -> str:
        return str(
            self._module_config()
            .get("parser", {})
            .get("alias_mapping_version", "core_financial_facts.v1")
        )

    def _core_fact_alias_mapping(self) -> Dict[str, List[str]]:
        parser_cfg = self._module_config().get("parser", {})
        mapping = get_core_financial_fact_aliases(self._alias_mapping_version())
        overrides = parser_cfg.get("core_fact_alias_overrides") or {}
        if not isinstance(overrides, dict):
            return mapping
        for core_field, aliases in overrides.items():
            if not isinstance(aliases, list):
                continue
            existing = mapping.setdefault(str(core_field), [])
            for alias in aliases:
                alias_text = str(alias).strip()
                if alias_text and alias_text not in existing:
                    existing.append(alias_text)
        return mapping

    def _fallback_parser_version(self, source: str) -> str:
        source_cfg = self.research_config.sources.get(source, {})
        financial_cfg = source_cfg.get("financial_statements", {})
        return str(
            financial_cfg.get(
                "parser_version",
                self._module_config()
                .get("parser", {})
                .get("parser_version", "financial_structured_filing.v1"),
            )
        )

    def _module_config(self) -> Dict[str, Any]:
        return self.research_config.modules.get("financial_statements", {})

    @staticmethod
    def _hash_payload(payload: Dict[str, Any]) -> str:
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    @staticmethod
    def _report_type_for_period(report_period: str) -> str:
        return _report_type_for_period(report_period)


def _parse_report_period(value: str) -> date:
    text = str(value).strip()
    if len(text) == 6 and text[4].upper() == "Q":
        year = int(text[:4])
        quarter = int(text[5])
        month, day = _QUARTER_ENDS[quarter]
        return date(year, month, day)
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return date.fromisoformat(text[:10])
    raise ValueError(f"unsupported financial report period: {value}")


def _date_to_quarter(value: date) -> tuple[int, int]:
    return value.year, ((value.month - 1) // 3) + 1


def _add_quarters(year: int, quarter: int, delta: int) -> tuple[int, int]:
    total = year * 4 + (quarter - 1) + delta
    return total // 4, (total % 4) + 1


def _format_quarter_period(year: int, quarter: int) -> str:
    month, day = _QUARTER_ENDS[quarter]
    return date(year, month, day).isoformat()


def _latest_disclosed_report_period(today: date) -> date:
    candidates: List[date] = []
    for year in range(today.year - 3, today.year + 1):
        for quarter in (1, 2, 3, 4):
            period = _parse_report_period(f"{year}Q{quarter}")
            deadline = _disclosure_deadline(year, quarter)
            if deadline <= today:
                candidates.append(period)
    if not candidates:
        return _parse_report_period(f"{today.year - 1}Q3")
    return max(candidates)


def _disclosure_deadline(year: int, quarter: int) -> date:
    if quarter == 1:
        return date(year, 4, 30)
    if quarter == 2:
        return date(year, 8, 31)
    if quarter == 3:
        return date(year, 10, 31)
    return date(year + 1, 4, 30)


def _report_type_for_period(report_period: str) -> str:
    _, quarter = _date_to_quarter(_parse_report_period(report_period))
    if quarter == 4:
        return "annual"
    return "quarterly"
