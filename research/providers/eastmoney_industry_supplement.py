"""
Eastmoney-backed industry-name supplement provider for strict Shenwan gaps.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional

import requests

from utils import dm_logger
from utils.http_transport import HttpTlsConfig, create_requests_session

from .base import BaseIndustryNameSupplementProvider, IndustryNameHintSnapshot


class EastmoneyIndustryNameSupplementProvider(BaseIndustryNameSupplementProvider):
    """Fetch stock-level industry-name hints from Eastmoney quote metadata."""

    source_name = "eastmoney"
    supported_modes = {"direct"}

    def __init__(
        self,
        *,
        endpoint: str = "https://push2.eastmoney.com/api/qt/stock/get",
        fields: str = "f57,f58,f127",
        request_timeout_seconds: float = 8.0,
        request_interval_seconds: float = 0.05,
        retry_attempts: int = 2,
        retry_backoff_seconds: float = 0.5,
        taxonomy_system: str = "sw",
        taxonomy_version: str = "sw_2021",
    ):
        self.endpoint = endpoint
        self.fields = fields
        self.request_timeout_seconds = max(1.0, float(request_timeout_seconds))
        self.request_interval_seconds = max(0.0, float(request_interval_seconds))
        self.retry_attempts = max(1, int(retry_attempts))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self.taxonomy_system = taxonomy_system
        self.taxonomy_version = taxonomy_version
        self.tls_config = HttpTlsConfig(source_name=self.source_name)

    async def fetch_industry_name_hints(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[IndustryNameHintSnapshot]:
        if not self.supports_mode(mode):
            return []

        target_instruments = [
            instrument
            for instrument in instruments
            if instrument.get("type") == "stock" and instrument.get("exchange") == exchange
        ]
        if limit is not None:
            target_instruments = target_instruments[:limit]
        if not target_instruments:
            return []

        return await asyncio.to_thread(
            self._fetch_industry_name_hints_sync,
            target_instruments,
            exchange,
            mode,
        )

    def _fetch_industry_name_hints_sync(
        self,
        target_instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str,
    ) -> List[IndustryNameHintSnapshot]:
        session = create_requests_session(tls_config=self.tls_config)
        hints: List[IndustryNameHintSnapshot] = []

        for index, instrument in enumerate(target_instruments):
            if index > 0 and self.request_interval_seconds > 0:
                time.sleep(self.request_interval_seconds)

            symbol = str(instrument.get("symbol") or "").strip()
            if not symbol:
                symbol = str(instrument.get("instrument_id") or "").split(".", 1)[0]
            if not symbol:
                continue

            raw_payload = self._fetch_one(session, symbol=symbol, exchange=exchange)
            if not raw_payload:
                continue

            industry_name = self._clean_text(raw_payload.get("f127"))
            if not industry_name:
                continue

            hints.append(
                IndustryNameHintSnapshot(
                    instrument_id=str(instrument.get("instrument_id") or ""),
                    symbol=symbol,
                    exchange=exchange,
                    taxonomy_system=self.taxonomy_system,
                    taxonomy_version=self.taxonomy_version,
                    industry_name=industry_name,
                    source_classification="东方财富个股行业",
                    source=self.source_name,
                    source_mode=mode,
                    raw_payload=raw_payload,
                )
            )

        return hints

    def _fetch_one(
        self,
        session: requests.Session,
        *,
        symbol: str,
        exchange: str,
    ) -> Dict[str, Any]:
        last_error: Optional[Exception] = None
        params = {
            "fltt": "2",
            "invt": "2",
            "fields": self.fields,
            "secid": self._to_eastmoney_secid(symbol=symbol, exchange=exchange),
        }
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Referer": "https://quote.eastmoney.com/",
        }

        for attempt in range(1, self.retry_attempts + 1):
            try:
                response = session.get(
                    self.endpoint,
                    params=params,
                    headers=headers,
                    timeout=self.request_timeout_seconds,
                )
                response.raise_for_status()
                data = response.json().get("data") or {}
                return data if isinstance(data, dict) else {}
            except Exception as exc:
                last_error = exc
                if attempt < self.retry_attempts and self.retry_backoff_seconds > 0:
                    time.sleep(self.retry_backoff_seconds * attempt)

        dm_logger.warning(
            "[EastmoneyIndustryNameSupplement] Failed to fetch industry hint for %s: %s",
            symbol,
            last_error,
        )
        return {}

    @staticmethod
    def _to_eastmoney_secid(*, symbol: str, exchange: str) -> str:
        normalized_exchange = str(exchange or "").upper()
        if normalized_exchange in {"SSE", "SH", "XSHG"}:
            return f"1.{symbol}"
        if normalized_exchange in {"SZSE", "SZ", "XSHE", "BSE", "BJ"}:
            return f"0.{symbol}"
        return f"{'1' if symbol.startswith('6') else '0'}.{symbol}"

    @staticmethod
    def _clean_text(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        if not text or text == "-":
            return None
        return text
