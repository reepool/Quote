"""Explicit remote-extension service for out-of-core financial facts."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from research.financial_statements_sync import (
    FinancialStatementsShadowSyncService,
    _report_type_for_period,
)
from research.providers.akshare_financial_statements import (
    AkshareFinancialStatementsProvider,
)
from research.providers.base import BaseFinancialStatementsProvider


class FinancialRemoteExtensionService:
    """Fetch non-local-core financial facts through an explicit remote path."""

    def __init__(
        self,
        *,
        provider: Optional[BaseFinancialStatementsProvider] = None,
        provider_config: Optional[Dict[str, Any]] = None,
    ):
        self.provider = provider or AkshareFinancialStatementsProvider(
            provider_config={
                "statement_interface_order": ["eastmoney_report"],
                **(provider_config or {}),
            }
        )

    async def fetch_facts(
        self,
        *,
        instrument: Dict[str, Any],
        exchange: str,
        requested_canonical_facts: List[str],
        report_periods: Optional[List[str]] = None,
        mode: str = "direct",
        allow_remote_extension: bool = False,
    ) -> Dict[str, Any]:
        """Return canonicalized remote facts only when explicitly allowed."""
        requested = [str(item) for item in requested_canonical_facts if str(item)]
        if not allow_remote_extension:
            return {
                "status": "remote_extension_not_allowed",
                "is_remote": True,
                "source": "akshare",
                "statement_interface": "eastmoney_report",
                "instrument_id": instrument.get("instrument_id"),
                "requested_canonical_facts": requested,
                "facts": [],
                "missing_fields": [
                    {
                        "canonical_fact": item,
                        "reason": "remote_extension_requires_explicit_opt_in",
                    }
                    for item in requested
                ],
            }

        bundles = await self.provider.fetch_financial_statement_bundles(
            instruments=[instrument],
            exchange=exchange,
            mode=mode,
            limit=1,
            report_periods=report_periods,
        )
        facts = []
        for bundle in bundles:
            numeric_facts = FinancialStatementsShadowSyncService._numeric_facts_from_fallback_bundle(
                bundle,
                source_file_id=f"remote:{bundle.instrument_id}:{bundle.report_period}",
                payload_hash="remote_extension_not_persisted",
                parser_version="akshare_financial_statements.remote_extension.v1",
            )
            for fact in numeric_facts:
                if fact.canonical_fact_name not in requested:
                    continue
                facts.append(
                    {
                        "instrument_id": fact.instrument_id,
                        "report_period": fact.report_period,
                        "report_type": fact.report_type
                        or _report_type_for_period(fact.report_period),
                        "canonical_fact_name": fact.canonical_fact_name,
                        "canonical_statement_family": fact.canonical_statement_family,
                        "canonical_semantic": fact.canonical_semantic,
                        "canonical_unit": fact.canonical_unit,
                        "source_field": fact.fact_name,
                        "source_unit": fact.unit,
                        "fact_value": fact.fact_value,
                        "source": fact.source,
                        "source_mode": fact.source_mode,
                        "parser_version": fact.parser_version,
                        "conversion": {
                            "unit_multiplier": 1.0,
                            "source_unit": fact.unit,
                            "canonical_unit": fact.canonical_unit,
                        },
                        "cache_status": "not_cached",
                        "is_remote": True,
                        "raw_fact": fact.raw_fact_json,
                    }
                )
        present = {fact["canonical_fact_name"] for fact in facts}
        missing_fields = [
            {
                "canonical_fact": item,
                "reason": "remote_extension_fact_not_returned",
            }
            for item in requested
            if item not in present
        ]
        return {
            "status": "passed" if facts and not missing_fields else "partial",
            "is_remote": True,
            "source": "akshare",
            "statement_interface": "eastmoney_report",
            "instrument_id": instrument.get("instrument_id"),
            "requested_canonical_facts": requested,
            "facts": facts,
            "missing_fields": missing_fields,
        }
