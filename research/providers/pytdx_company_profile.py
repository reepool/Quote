"""
pytdx-backed company profile provider.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, Iterable, List, Optional

from .base import BaseCompanyProfileProvider, CompanyProfileSnapshot
from .pytdx_support import EXCHANGE_TO_MARKET, MARKET_TO_EXCHANGE, PytdxProviderMixin


class PytdxCompanyProfileProvider(PytdxProviderMixin, BaseCompanyProfileProvider):
    """Fetch company profile snapshots from pytdx company materials."""

    source_name = "pytdx"
    supported_modes = {"direct"}
    DEFAULT_CATEGORY_KEYWORDS = (
        "公司概况",
        "公司简介",
        "公司资料",
        "基本资料",
        "股本结构",
        "股东研究",
        "十大股东",
        "经营分析",
        "最新提示",
    )

    def __init__(
        self,
        *,
        connection_pool: Any = None,
        hosts: Optional[list[tuple[str, str, int]]] = None,
        connection_timeout: float = 10.0,
        max_connect_attempts: int = 5,
        category_keywords: Optional[Iterable[str]] = None,
        max_content_categories: int = 4,
        max_content_length: int = 4000,
    ):
        super().__init__(
            connection_pool=connection_pool,
            hosts=hosts,
            connection_timeout=connection_timeout,
            max_connect_attempts=max_connect_attempts,
        )
        self._category_keywords = tuple(category_keywords or self.DEFAULT_CATEGORY_KEYWORDS)
        self._max_content_categories = max_content_categories
        self._max_content_length = max_content_length

    async def fetch_company_profiles(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[CompanyProfileSnapshot]:
        if not self.supports_mode(mode):
            return []

        target_instruments = [
            instrument for instrument in instruments
            if instrument.get("type") == "stock" and instrument.get("exchange") == exchange
        ]
        if limit is not None:
            target_instruments = target_instruments[:limit]

        if not target_instruments:
            return []

        return await asyncio.to_thread(
            self._fetch_company_profiles_sync,
            target_instruments,
            exchange,
        )

    def _fetch_company_profiles_sync(
        self,
        target_instruments: List[Dict[str, Any]],
        exchange: str,
    ) -> List[CompanyProfileSnapshot]:
        api, managed_connection = self._get_api()
        expected_market = EXCHANGE_TO_MARKET.get(exchange)
        snapshots: List[CompanyProfileSnapshot] = []

        try:
            for instrument in target_instruments:
                instrument_id = instrument.get("instrument_id", "")
                parsed = self.parse_instrument_id(instrument_id)
                if parsed is None:
                    continue

                market, code = parsed
                if expected_market is not None and market != expected_market:
                    continue

                finance_info = self._normalize_mapping(api.get_finance_info(market, code))
                categories = self._normalize_categories(api.get_company_info_category(market, code))
                content_map = self._fetch_company_material(api, market, code, categories)

                if not finance_info and not categories and not content_map:
                    continue

                company_name = self._pick_company_name(finance_info, instrument, instrument_id)
                raw_payload: Dict[str, Any] = {}
                if finance_info:
                    raw_payload["finance_info"] = finance_info
                if categories:
                    raw_payload["company_info_categories"] = categories
                if content_map:
                    raw_payload["company_info_content"] = content_map

                snapshots.append(
                    CompanyProfileSnapshot(
                        instrument_id=instrument_id,
                        symbol=instrument.get("symbol", code),
                        company_name=company_name,
                        short_name=instrument.get("name", company_name),
                        exchange=instrument.get("exchange", exchange),
                        market=MARKET_TO_EXCHANGE.get(market, str(market)),
                        listed_date=self.normalize_numeric_date(
                            finance_info.get("ipoDate")
                            or finance_info.get("listing_date")
                            or finance_info.get("上市日期")
                            or finance_info.get("上市时间")
                        ),
                        industry_raw=instrument.get("industry"),
                        sector_raw=instrument.get("sector"),
                        status="active" if instrument.get("is_active", True) else "delisted",
                        source=self.source_name,
                        source_mode="direct",
                        raw_payload=raw_payload,
                    )
                )
        finally:
            if managed_connection:
                try:
                    api.disconnect()
                except Exception:
                    pass

        return snapshots

    def _fetch_company_material(
        self,
        api: Any,
        market: int,
        code: str,
        categories: List[Dict[str, Any]],
    ) -> Dict[str, str]:
        content_map: Dict[str, str] = {}
        for category in self._select_categories(categories):
            filename = str(category.get("filename", "")).strip()
            start = self._coerce_int(category.get("start"))
            length = self._coerce_int(category.get("length"))
            name = str(category.get("name", "")).strip() or filename

            if not filename or start is None or length is None or length <= 0:
                continue

            content = api.get_company_info_content(market, code, filename, start, length)
            normalized = self._normalize_text(content)
            if not normalized:
                continue

            content_map[name] = normalized

        return content_map

    def _select_categories(self, categories: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        selected = [
            category for category in categories
            if any(
                keyword in str(category.get("name", ""))
                for keyword in self._category_keywords
            )
        ]
        if not selected:
            selected = categories[:self._max_content_categories]
        return selected[:self._max_content_categories]

    @staticmethod
    def _normalize_categories(rows: Any) -> List[Dict[str, Any]]:
        if not rows:
            return []

        categories: List[Dict[str, Any]] = []
        for row in rows:
            if hasattr(row, "items"):
                categories.append(dict(row.items()))
            elif isinstance(row, dict):
                categories.append(dict(row))
        return categories

    @staticmethod
    def _normalize_mapping(value: Any) -> Dict[str, Any]:
        if not value:
            return {}
        if hasattr(value, "items"):
            return dict(value.items())
        if isinstance(value, dict):
            return dict(value)
        return {}

    def _normalize_text(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (bytes, bytearray)):
            try:
                text = value.decode("gbk")
            except UnicodeDecodeError:
                text = value.decode("utf-8", errors="ignore")
        else:
            text = str(value)

        text = text.strip()
        if not text:
            return ""

        if len(text) > self._max_content_length:
            return text[:self._max_content_length]
        return text

    @staticmethod
    def _coerce_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _pick_company_name(
        finance_info: Dict[str, Any],
        instrument: Dict[str, Any],
        fallback: str,
    ) -> str:
        for key in ("name", "证券简称", "证券名称", "code_name"):
            value = finance_info.get(key)
            if value:
                return str(value)
        return instrument.get("name", fallback)
