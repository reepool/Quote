"""
Configured official structured financial filing provider.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from typing import Any, Dict, List, Optional

import requests

from .base import (
    BaseOfficialFinancialFilingProvider,
    FinancialFilingPayload,
    FinancialSourceFileManifest,
)


class ConfiguredOfficialFinancialFilingProvider(BaseOfficialFinancialFilingProvider):
    """Fetch official filing payloads from configured URL templates."""

    supported_modes = {"direct"}

    def __init__(
        self,
        *,
        source_name: str,
        source_config: Dict[str, Any],
        session: Optional[requests.Session] = None,
    ):
        self.source_name = source_name
        self.source_config = source_config
        self.endpoint_url = str(source_config.get("endpoint_url") or "")
        self.timeout = float(source_config.get("request_timeout_seconds", 20.0))
        self.request_interval = float(source_config.get("request_interval_seconds", 0.0))
        self.session = session or requests.Session()

    async def fetch_financial_filings(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        report_periods: List[str],
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[FinancialFilingPayload]:
        """Fetch structured filing payloads for configured endpoint templates."""
        if not self.supports_mode(mode) or not self.endpoint_url:
            return []
        target_instruments = [
            item
            for item in instruments
            if item.get("exchange") == exchange and item.get("type", "stock") == "stock"
        ]
        if limit is not None:
            target_instruments = target_instruments[:limit]
        if not target_instruments or not report_periods:
            return []

        return await asyncio.to_thread(
            self._fetch_sync,
            target_instruments,
            exchange,
            report_periods,
            mode,
        )

    def _fetch_sync(
        self,
        instruments: List[Dict[str, Any]],
        exchange: str,
        report_periods: List[str],
        mode: str,
    ) -> List[FinancialFilingPayload]:
        payloads: List[FinancialFilingPayload] = []
        for instrument in instruments:
            for report_period in report_periods:
                context = self._context(instrument, exchange, report_period)
                url = self.endpoint_url.format_map(_SafeFormatDict(context))
                response = self.session.get(
                    url,
                    timeout=self.timeout,
                    headers={"User-Agent": "QuoteResearch/official-financial-filing"},
                )
                response.raise_for_status()
                content = bytes(response.content or b"")
                content_hash = hashlib.sha256(content).hexdigest()
                manifest = FinancialSourceFileManifest(
                    source=self.source_name,
                    source_mode=mode,
                    instrument_id=str(instrument.get("instrument_id") or ""),
                    symbol=str(instrument.get("symbol") or context["symbol"]),
                    exchange=exchange,
                    report_period=report_period,
                    report_type=str(self.source_config.get("report_type") or ""),
                    source_url=url,
                    content_hash=content_hash,
                    content_length=len(content),
                    parser_version=str(
                        self.source_config.get(
                            "parser_version",
                            "financial_structured_filing.v1",
                        )
                    ),
                    status="downloaded",
                    metadata_json={
                        "content_type": response.headers.get("Content-Type"),
                    },
                )
                payloads.append(
                    FinancialFilingPayload(
                        manifest=manifest,
                        content=content,
                        text=response.text,
                        content_type=response.headers.get("Content-Type"),
                    )
                )
                if self.request_interval > 0:
                    time.sleep(self.request_interval)
        return payloads

    @staticmethod
    def _context(
        instrument: Dict[str, Any],
        exchange: str,
        report_period: str,
    ) -> Dict[str, str]:
        instrument_id = str(instrument.get("instrument_id") or "")
        symbol = str(instrument.get("symbol") or instrument_id.split(".")[0])
        return {
            "instrument_id": instrument_id,
            "symbol": symbol,
            "stockid": symbol,
            "exchange": exchange,
            "report_period": report_period,
        }


class _SafeFormatDict(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"
