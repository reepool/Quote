"""
Sina-backed structured Shenwan industry supplement provider.
"""

from __future__ import annotations

import asyncio
import re
import time
from html import unescape
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional

import requests

from utils import dm_logger
from utils.http_transport import HttpTlsConfig, create_requests_session

from .base import BaseIndustryNameSupplementProvider, IndustryNameHintSnapshot


class _TableRowTextParser(HTMLParser):
    """Extract table rows as plain text cells from Sina's legacy HTML pages."""

    def __init__(self) -> None:
        super().__init__()
        self.rows: List[List[str]] = []
        self._in_row = False
        self._in_cell = False
        self._current_row: List[str] = []
        self._current_cell_parts: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        if tag.lower() == "tr":
            self._in_row = True
            self._current_row = []
        elif self._in_row and tag.lower() in {"td", "th"}:
            self._in_cell = True
            self._current_cell_parts = []

    def handle_endtag(self, tag: str) -> None:
        normalized_tag = tag.lower()
        if normalized_tag in {"td", "th"} and self._in_cell:
            cell_text = _clean_cell_text("".join(self._current_cell_parts))
            self._current_row.append(cell_text)
            self._in_cell = False
            self._current_cell_parts = []
        elif normalized_tag == "tr" and self._in_row:
            if any(cell for cell in self._current_row):
                self.rows.append(self._current_row)
            self._in_row = False
            self._current_row = []

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._current_cell_parts.append(data)

    def handle_entityref(self, name: str) -> None:
        if self._in_cell:
            self._current_cell_parts.append(unescape(f"&{name};"))

    def handle_charref(self, name: str) -> None:
        if self._in_cell:
            self._current_cell_parts.append(unescape(f"&#{name};"))


class SinaIndustryNameSupplementProvider(BaseIndustryNameSupplementProvider):
    """Fetch Shenwan leaf hints from Sina Finance related-data pages."""

    source_name = "sina"
    supported_modes = {"direct"}

    _GENERIC_SHENWAN_NAME_PATTERNS = (
        "申万A指",
        "申万Ａ指",
        "申万300",
        "申万制造",
        "申万重点",
        "申万投资",
        "申万价值",
        "指数",
        "基金重仓",
    )

    def __init__(
        self,
        *,
        endpoint_template: str = (
            "https://vip.stock.finance.sina.com.cn/corp/go.php/"
            "vCI_CorpXiangGuan/stockid/{stockid}.phtml"
        ),
        request_timeout_seconds: float = 8.0,
        request_interval_seconds: float = 0.1,
        retry_attempts: int = 2,
        retry_backoff_seconds: float = 0.5,
        taxonomy_system: str = "sw",
        taxonomy_version: str = "sw_2021",
    ):
        self.endpoint_template = endpoint_template
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

            for candidate in raw_payload.get("candidates", []):
                industry_name = str(candidate.get("industry_name") or "").strip()
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
                        source_classification="新浪财经相关资料",
                        source=self.source_name,
                        source_mode=mode,
                        raw_payload={
                            "source_page": raw_payload.get("source_page"),
                            "selected_candidate": candidate,
                            "candidate_count": len(raw_payload.get("candidates", [])),
                        },
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
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Referer": "https://finance.sina.com.cn/",
        }

        for stockid in self._stockid_variants(symbol=symbol, exchange=exchange):
            url = self.endpoint_template.format(stockid=stockid)
            for attempt in range(1, self.retry_attempts + 1):
                try:
                    response = session.get(
                        url,
                        headers=headers,
                        timeout=self.request_timeout_seconds,
                    )
                    response.raise_for_status()
                    html_text = self._decode_response(response)
                    candidates = self._parse_shenwan_candidates(html_text)
                    if candidates:
                        return {
                            "source_page": url,
                            "stockid": stockid,
                            "candidates": candidates,
                        }
                    break
                except Exception as exc:
                    last_error = exc
                    if attempt < self.retry_attempts and self.retry_backoff_seconds > 0:
                        time.sleep(self.retry_backoff_seconds * attempt)

        dm_logger.warning(
            "[SinaIndustryNameSupplement] Failed to fetch Shenwan hints for %s: %s",
            symbol,
            last_error,
        )
        return {}

    @classmethod
    def _parse_shenwan_candidates(cls, html_text: str) -> List[Dict[str, Any]]:
        parser = _TableRowTextParser()
        parser.feed(html_text or "")

        candidates: List[Dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for row in parser.rows:
            if len(row) < 2:
                continue
            name = _clean_cell_text(row[0])
            code = _clean_code(row[1])
            if not name or not cls._is_shenwan_industry_code(code):
                continue
            if cls._is_generic_shenwan_index_name(name):
                continue

            enter_date = _clean_cell_text(row[2]) if len(row) >= 3 else ""
            exit_date = _clean_cell_text(row[3]) if len(row) >= 4 else ""
            is_current = not _looks_like_date(exit_date)
            if not is_current:
                continue

            key = (name, code)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                {
                    "industry_name": name,
                    "industry_code": f"{code}.SI",
                    "raw_industry_code": code,
                    "enter_date": enter_date or None,
                    "exit_date": exit_date or None,
                    "source_row": row,
                    "priority": cls._candidate_priority(name=name, code=code),
                }
            )

        candidates.sort(
            key=lambda item: (
                int(item.get("priority", 99)),
                str(item.get("industry_code") or ""),
            )
        )
        for item in candidates:
            item.pop("priority", None)
        return candidates

    @classmethod
    def _candidate_priority(cls, *, name: str, code: str) -> int:
        if code.startswith("85"):
            return 0
        if name.endswith(("Ⅲ", "III")):
            return 1
        if name.endswith(("Ⅱ", "II")):
            return 2
        if code.startswith("801"):
            return 3
        return 9

    @classmethod
    def _is_shenwan_industry_code(cls, code: str) -> bool:
        return bool(re.fullmatch(r"(801\d{3}|85\d{4})", code or ""))

    @classmethod
    def _is_generic_shenwan_index_name(cls, name: str) -> bool:
        compact_name = name.replace(" ", "")
        return any(pattern in compact_name for pattern in cls._GENERIC_SHENWAN_NAME_PATTERNS)

    @staticmethod
    def _stockid_variants(*, symbol: str, exchange: str) -> List[str]:
        normalized_exchange = str(exchange or "").upper()
        prefix = ""
        if normalized_exchange in {"SSE", "SH", "XSHG"}:
            prefix = "sh"
        elif normalized_exchange in {"SZSE", "SZ", "XSHE"}:
            prefix = "sz"
        elif normalized_exchange in {"BSE", "BJ"}:
            prefix = "bj"

        variants: List[str] = []
        if prefix:
            variants.append(f"{prefix}{symbol}")
        variants.append(symbol)
        return list(dict.fromkeys(variants))

    @staticmethod
    def _decode_response(response: requests.Response) -> str:
        encoding = response.encoding or getattr(response, "apparent_encoding", None) or "gb18030"
        try:
            response.encoding = encoding
            return response.text
        except Exception:
            return response.content.decode("gb18030", errors="ignore")


def _clean_cell_text(value: Any) -> str:
    text = unescape(str(value or ""))
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def _clean_code(value: Any) -> str:
    text = _clean_cell_text(value)
    text = text.split(".", 1)[0]
    return re.sub(r"\D", "", text)


def _looks_like_date(value: str) -> bool:
    text = _clean_cell_text(value)
    return bool(re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}", text))
